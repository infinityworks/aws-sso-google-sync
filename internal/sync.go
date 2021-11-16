// Copyright (c) 2020, Amazon.com, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package internal ...
package internal

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/infinityworks/aws-sso-google-sync/internal/aws"
	"github.com/infinityworks/aws-sso-google-sync/internal/config"
	"github.com/infinityworks/aws-sso-google-sync/internal/google"

	log "github.com/sirupsen/logrus"
	admin "google.golang.org/api/admin/directory/v1"
)

// SyncGSuite is the interface for synchronizing users/groups
type SyncGSuite interface {
	SyncGroupsUsers(string) error
}

// SyncGSuite is an object type that will synchronize real users and groups
type syncGSuite struct {
	aws         aws.SCIMClient
	awsDynamoDB aws.DynamoDBClient
	google      google.Client
	cfg         *config.Config

	users map[string]*aws.User
}

// New will create a new SyncGSuite object
func New(cfg *config.Config, a aws.SCIMClient, d aws.DynamoDBClient, g google.Client) SyncGSuite {
	return &syncGSuite{
		aws:         a,
		awsDynamoDB: d,
		google:      g,
		cfg:         cfg,
		users:       make(map[string]*aws.User),
	}
}

// SyncGroupsUsers will sync groups and its members from Google -> AWS SSO SCIM
// allowing filter groups base on google api filter query parameter
// References:
// * https://developers.google.com/admin-sdk/directory/v1/guides/search-groups
// query possible values:
// '' --> empty or not defined
//  name='contact'
//  email:admin*
//  memberKey=user@company.com
//  name:contact* email:contact*
//  name:Admin* email:aws-*
//  email:aws-*
// process workflow:
//  1) delete users in aws, these were deleted in google
//  2) update users in aws, these were updated in google
//  3) add users in aws, these were added in google
//  4) add groups in aws and add its members, these were added in google
//  5) validate equals aws an google groups members
//  6) delete groups in aws, these were deleted in google
func (s *syncGSuite) SyncGroupsUsers(query string) error {

	log.WithField("query", query).Info("get google groups")
	googleGroups, err := s.google.GetGroups(query)
	if err != nil {
		return err
	}
	filteredGoogleGroups := []*admin.Group{}
	for _, g := range googleGroups {
		if s.ignoreGroup(g.Email) {
			log.WithField("group", g.Email).Debug("ignoring group")
			continue
		}
		filteredGoogleGroups = append(filteredGoogleGroups, g)
	}
	googleGroups = filteredGoogleGroups

	log.Debug("preparing list of google users and then google groups and their members")
	googleUsers, googleGroupsUsers, err := s.getGoogleGroupsAndUsers(googleGroups)
	if err != nil {
		return err
	}

	log.Debug("getting existing sso groups and users from dynamodb")
	awsGroups, err := s.awsDynamoDB.GetGroupsWithMembers()
	if err != nil {
		log.Error("error getting aws groups and users from dynamodb")
		return err
	}

	var awsUserEmails []string
	for _, group := range awsGroups {
		awsUserEmails = append(awsUserEmails, group.Members...)
	}

	var awsUsers []*aws.User
	for _, awsUserEmail := range awsUserEmails {
		awsUser, err := s.aws.FindUserByEmail(awsUserEmail)
		if err != nil {
			// todo - reconcile dynamodb and sso?
			log.WithFields(log.Fields{"userEmail": awsUserEmail}).Error("error getting aws user from aws sso")
			return err
		}
		awsUsers = append(awsUsers, awsUser)
	}

	awsGroupsUsers, err := s.getAWSGroupsAndUsers(awsGroups, awsUsers)

	// create list of changes by operations
	addAWSUsers, delAWSUsers, updateAWSUsers, _ := getUserOperations(awsUsers, googleUsers)
	addAWSGroups, delAWSGroups, equalAWSGroups := getGroupOperations(awsGroups, googleGroups)

	if s.cfg.DryRun {
		log.Info("running in dry run mode. skipping apply.")
		return nil
	}

	log.Info("syncing changes")
	// delete aws users (deleted in google)
	log.Debug("deleting aws users deleted in google")
	for _, awsUser := range delAWSUsers {

		log := log.WithFields(log.Fields{"user": awsUser.Username})

		log.Debug("finding user")
		awsUserFull, err := s.aws.FindUserByEmail(awsUser.Username)
		if err != nil {
			return err
		}

		log.Warn("deleting user")
		if err := s.aws.DeleteUser(awsUserFull); err != nil {
			log.Error("error deleting user")
			return err
		}
	}

	// update aws users (updated in google)
	log.Debug("updating aws users updated in google")
	for _, awsUser := range updateAWSUsers {

		log := log.WithFields(log.Fields{"user": awsUser.Username})

		log.Debug("finding user")
		awsUserFull, err := s.aws.FindUserByEmail(awsUser.Username)
		if err != nil {
			return err
		}

		log.Warn("updating user")
		_, err = s.aws.UpdateUser(awsUserFull)
		if err != nil {
			log.Error("error updating user")
			return err
		}
	}

	// add aws users (added in google)
	log.Debug("creating aws users added in google")
	for _, awsUser := range addAWSUsers {

		log := log.WithFields(log.Fields{"user": awsUser.Username})

		log.Info("creating user")
		_, err := s.aws.CreateUser(awsUser)
		if err != nil {
			log.Error("error creating user")
			return err
		}
	}

	// add aws groups (added in google)
	log.Debug("creating aws groups added in google")
	for _, awsGroup := range addAWSGroups {

		log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})

		log.Info("creating group")
		_, err := s.aws.CreateGroup(awsGroup)
		if err != nil {
			log.Error("creating group")
			return err
		}

	}

	newAwsGroups, err := s.aws.GetGroups()
	if err != nil {
		return err
	}

	for _, newAwsGroup := range newAwsGroups {
		if _, ok := googleGroupsUsers[newAwsGroup.DisplayName]; !ok {
			log.Debug("aws group not present in google group. skipping...")
			continue
		}

		// add members of the new group
		for _, googleUser := range googleGroupsUsers[newAwsGroup.DisplayName] {

			// equivalent aws user of google user on the fly
			log.Debug("finding user")
			awsUserFull, err := s.aws.FindUserByEmail(googleUser.PrimaryEmail)
			if err != nil {
				return err
			}

			log.WithField("user", awsUserFull.Username).Info("adding user to group")
			err = s.aws.AddUserToGroup(awsUserFull, newAwsGroup)
			if err != nil {
				return err
			}
		}
	}

	// list of users to to be removed in aws groups
	deleteUsersFromGroup, _ := getGroupUsersOperations(googleGroupsUsers, awsGroupsUsers)

	// validate groups members are equal in aws and google
	log.Debug("validating groups members, equals in aws and google")
	for _, awsGroup := range equalAWSGroups {

		// add members of the new group
		log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})

		for _, googleUser := range googleGroupsUsers[awsGroup.DisplayName] {

			log.WithField("user", googleUser.PrimaryEmail).Debug("finding user")
			awsUserFull, err := s.aws.FindUserByEmail(googleUser.PrimaryEmail)
			if err != nil {
				return err
			}

			log.WithField("user", awsUserFull.Username).Debug("checking user is in group already")
			b, err := s.aws.IsUserInGroup(awsUserFull, awsGroup)
			if err != nil {
				return err
			}

			if !b {
				log.WithField("user", awsUserFull.Username).Info("adding user to group")
				err := s.aws.AddUserToGroup(awsUserFull, awsGroup)
				if err != nil {
					return err
				}
			}
		}

		for _, awsUser := range deleteUsersFromGroup[awsGroup.DisplayName] {
			log.WithField("user", awsUser.Username).Warn("removing user from group")
			err := s.aws.RemoveUserFromGroup(awsUser, awsGroup)
			if err != nil {
				return err
			}
		}
	}

	// delete aws groups (deleted in google)
	log.Debug("delete aws groups deleted in google")
	for _, awsGroup := range delAWSGroups {

		log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})

		log.Debug("finding group")
		awsGroupFull, err := s.aws.FindGroupByDisplayName(awsGroup.DisplayName)
		if err != nil {
			return err
		}

		log.Warn("deleting group")
		err = s.aws.DeleteGroup(awsGroupFull)
		if err != nil {
			log.Error("deleting group")
			return err
		}
	}

	log.Info("sync completed")

	return nil
}

// getGoogleGroupsAndUsers return a list of google users members of googleGroups
// and a map of google groups and its users' list
func (s *syncGSuite) getGoogleGroupsAndUsers(googleGroups []*admin.Group) ([]*admin.User, map[string][]*admin.User, error) {
	gUsers := make([]*admin.User, 0)
	gGroupsUsers := make(map[string][]*admin.User)

	gUniqUsers := make(map[string]*admin.User)

	for _, g := range googleGroups {

		log := log.WithFields(log.Fields{"group": g.Name})

		if s.ignoreGroup(g.Email) {
			log.Debug("ignoring group")
			continue
		}

		log.Debug("get group members from google")
		groupMembers, err := s.google.GetGroupMembers(g)
		if err != nil {
			return nil, nil, err
		}

		log.Debug("get users")
		membersUsers := make([]*admin.User, 0)

		for _, m := range groupMembers {

			if s.ignoreUser(m.Email) {
				log.WithField("id", m.Email).Debug("ignoring user")
				continue
			}

			if m.Type == "GROUP" {
				log.WithField("id", m.Email).Debug("ignoring group address")
				continue
			}

			log.WithField("id", m.Email).Debug("get user")
			q := fmt.Sprintf("email:%s", m.Email)
			u, err := s.google.GetUsers(q) // TODO: implement GetUser(m.Email)
			if err != nil {
				return nil, nil, err
			}

			if len(u) == 0 {
				log.WithField("email", m.Email).Debug("Ignoring Unknown User")
				continue
			}

			membersUsers = append(membersUsers, u[0])

			_, ok := gUniqUsers[m.Email]
			if !ok {
				gUniqUsers[m.Email] = u[0]
			}
		}
		gGroupsUsers[g.Name] = membersUsers
	}

	for _, user := range gUniqUsers {
		gUsers = append(gUsers, user)
	}

	return gUsers, gGroupsUsers, nil
}

// getAWSGroupsAndUsers return a list of google users members of googleGroups
// and a map of google groups and its users' list
func (s *syncGSuite) getAWSGroupsAndUsers(awsGroups []*aws.Group, awsUsers []*aws.User) (map[string][]*aws.User, error) {
	awsGroupsUsers := make(map[string][]*aws.User)

	for _, awsGroup := range awsGroups {

		users := make([]*aws.User, 0)
		log := log.WithFields(log.Fields{"group": awsGroup.DisplayName})

		log.Debug("get group members from aws")
		// NOTE: AWS has not implemented yet some method to get the groups members https://docs.aws.amazon.com/singlesignon/latest/developerguide/listgroups.html
		// so, we need to check each user in each group which are too many unnecessary API calls
		for _, user := range awsUsers {

			log.Debug("checking if user is member of")
			found, err := s.aws.IsUserInGroup(user, awsGroup)
			if err != nil {
				return nil, err
			}
			if found {
				users = append(users, user)
			}
		}

		awsGroupsUsers[awsGroup.DisplayName] = users
	}
	return awsGroupsUsers, nil
}

// getGroupOperations returns the groups of AWS that must be added, deleted and are equals
func getGroupOperations(awsGroups []*aws.Group, googleGroups []*admin.Group) (add []*aws.Group, delete []*aws.Group, equals []*aws.Group) {

	awsMap := make(map[string]*aws.Group)
	googleMap := make(map[string]struct{})

	for _, awsGroup := range awsGroups {
		awsMap[awsGroup.DisplayName] = awsGroup
	}

	for _, gGroup := range googleGroups {
		googleMap[gGroup.Name] = struct{}{}
	}

	// AWS Groups found and not found in google
	for _, gGroup := range googleGroups {
		if _, found := awsMap[gGroup.Name]; found {
			log.WithFields(log.Fields{"group": gGroup.Name}).Debug("no changes to group")
			equals = append(equals, awsMap[gGroup.Name])
		} else {
			log.WithFields(log.Fields{"group": gGroup.Name}).Debug("adding group")
			add = append(add, aws.NewGroup(gGroup.Name))
		}
	}

	// Google Groups founds and not in aws
	for _, awsGroup := range awsGroups {
		if _, found := googleMap[awsGroup.DisplayName]; !found {
			log.WithFields(log.Fields{"group": awsGroup.DisplayName}).Debug("deleting group")
			delete = append(delete, aws.NewGroup(awsGroup.DisplayName))
		}
	}

	return add, delete, equals
}

// getUserOperations returns the users of AWS that must be added, deleted, updated and are equals
func getUserOperations(awsUsers []*aws.User, googleUsers []*admin.User) (add []*aws.User, delete []*aws.User, update []*aws.User, equals []*aws.User) {

	awsMap := make(map[string]*aws.User)
	googleMap := make(map[string]struct{})

	for _, awsUser := range awsUsers {
		awsMap[awsUser.Username] = awsUser
	}

	for _, gUser := range googleUsers {
		googleMap[gUser.PrimaryEmail] = struct{}{}
	}

	for _, gUser := range googleUsers {
		// Google Users found and found in AWS
		if awsUser, found := awsMap[gUser.PrimaryEmail]; found {
			if awsUser.Active == gUser.Suspended ||
				awsUser.Name.GivenName != gUser.Name.GivenName ||
				awsUser.Name.FamilyName != gUser.Name.FamilyName {
				log.WithFields(log.Fields{"user": awsUser.Username}).Debug("updating user")
				update = append(update, aws.NewUser(gUser.Name.GivenName, gUser.Name.FamilyName, gUser.PrimaryEmail, !gUser.Suspended))
			} else {
				log.WithFields(log.Fields{"user": awsUser.Username}).Debug("no changes to user")
				equals = append(equals, awsUser)
			}
		} else {
			// Google Users found and not found in AWS
			log.WithFields(log.Fields{"user": gUser.PrimaryEmail}).Debug("adding user")
			add = append(add, aws.NewUser(gUser.Name.GivenName, gUser.Name.FamilyName, gUser.PrimaryEmail, !gUser.Suspended))
		}
	}

	// AWS Users founds and not in Google
	for _, awsUser := range awsUsers {
		if _, found := googleMap[awsUser.Username]; !found {
			log.WithFields(log.Fields{"user": awsUser.Username}).Debug("deleting user")
			delete = append(delete, aws.NewUser(awsUser.Name.GivenName, awsUser.Name.FamilyName, awsUser.Username, awsUser.Active))
		}
	}

	return add, delete, update, equals
}

// groupUsersOperations returns the groups and its users of AWS that must be delete from these groups and what are equals
func getGroupUsersOperations(gGroupsUsers map[string][]*admin.User, awsGroupsUsers map[string][]*aws.User) (delete map[string][]*aws.User, equals map[string][]*aws.User) {

	mbG := make(map[string]map[string]struct{})

	// get user in google groups that are in aws groups and
	// users in aws groups that aren't in google groups
	for gGroupName, gGroupUsers := range gGroupsUsers {
		mbG[gGroupName] = make(map[string]struct{})
		for _, gUser := range gGroupUsers {
			mbG[gGroupName][gUser.PrimaryEmail] = struct{}{}
		}
	}

	delete = make(map[string][]*aws.User)
	equals = make(map[string][]*aws.User)
	for awsGroupName, awsGroupUsers := range awsGroupsUsers {
		for _, awsUser := range awsGroupUsers {
			// users that exist in aws groups but doesn't in google groups
			if _, found := mbG[awsGroupName][awsUser.Username]; found {
				equals[awsGroupName] = append(equals[awsGroupName], awsUser)
			} else {
				delete[awsGroupName] = append(delete[awsGroupName], awsUser)
			}
		}
	}

	return
}

// DoSync will create a logger and run the sync with the paths
// given to do the sync.
func DoSync(ctx context.Context, cfg *config.Config) error {
	log.Info("Syncing AWS users and groups from Google Workspace SAML Application")

	creds := []byte(cfg.GoogleCredentials)

	if !cfg.IsLambda {
		b, err := ioutil.ReadFile(cfg.GoogleCredentials)
		if err != nil {
			return err
		}
		creds = b
	}

	// create a http client with retry and backoff capabilities
	retryClient := retryablehttp.NewClient()

	// https://github.com/hashicorp/go-retryablehttp/issues/6
	if cfg.Debug {
		retryClient.Logger = log.StandardLogger()
	} else {
		retryClient.Logger = nil
	}

	httpClient := retryClient.StandardClient()

	googleClient, err := google.NewClient(ctx, cfg.GoogleAdmin, creds)
	if err != nil {
		return err
	}

	awsSCIMClient, err := aws.NewSCIMClient(
		httpClient,
		&aws.SCIMConfig{
			Endpoint: cfg.SCIMEndpoint,
			Token:    cfg.SCIMAccessToken,
		})
	if err != nil {
		return err
	}

	awsDynamoDBClient, err := aws.NewDynamoDBClient(&aws.DynamoDBConfig{
		DynamoDBTable: cfg.DynamoDBTable,
	})

	c := New(cfg, awsSCIMClient, awsDynamoDBClient, googleClient)

	err = c.SyncGroupsUsers(cfg.GroupMatch)
	if err != nil {
		return err
	}

	return nil
}

func (s *syncGSuite) ignoreUser(name string) bool {
	for _, u := range s.cfg.IgnoreUsers {
		if u == name {
			return true
		}
	}

	return false
}

func (s *syncGSuite) ignoreGroup(name string) bool {
	for _, g := range s.cfg.IgnoreGroups {
		if g == name {
			return true
		}
	}

	return false
}

func (s *syncGSuite) includeGroup(name string) bool {
	for _, g := range s.cfg.IncludeGroups {
		if g == name {
			return true
		}
	}

	return false
}
