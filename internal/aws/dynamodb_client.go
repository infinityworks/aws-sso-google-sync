package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	log "github.com/sirupsen/logrus"
)

type DynamoDBConfig struct {
	DynamoDBTableUsers  string
	DynamoDBTableGroups string
}

type DynamoDBGroupUser struct {
	GroupName string `json:"groupName"`
	Username  string `json:"username"`
}

type DynamoDBClient interface {
	GetGroups() ([]*Group, error)
	GetGroupMembers(*Group) ([]*User, error)
	GetUsers() ([]*User, error)
	AddUserToGroup(*User, *Group) error
	RemoveUserFromGroup(*User, *Group) error
	CreateUser(*User) error
	DeleteUser(*User) error
	IsUserInGroup(*User, *Group) (bool, error)
}

type dynamoDBClient struct {
	client *dynamodb.DynamoDB
	config *DynamoDBConfig
}

func NewDynamoDBClient(config *DynamoDBConfig) DynamoDBClient {
	session := session.Must(session.NewSession())
	client := dynamodb.New(session)

	return &dynamoDBClient{
		client: client,
		config: config,
	}
}

func (c *dynamoDBClient) GetGroups() ([]*Group, error) {
	params := &dynamodb.ScanInput{
		TableName: aws.String(c.config.DynamoDBTableGroups),
	}

	// todo - check whether scan is paginated
	result, err := c.client.Scan(params)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Query API call failed: %s", err)
		return nil, err
	}

	groupUsers := []*DynamoDBGroupUser{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &groupUsers)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Unmarshaling dynamodb response failed: %s", err)
		return nil, err
	}

	groupNames := map[string]struct{}{}
	for _, groupUser := range groupUsers {
		if _, ok := groupNames[groupUser.GroupName]; !ok {
			groupNames[groupUser.GroupName] = struct{}{}
		}
	}

	groups := []*Group{}
	for groupName, _ := range groupNames {
		groups = append(groups, &Group{
			DisplayName: groupName,
		})
	}
	return groups, nil
}

func (c *dynamoDBClient) GetGroupMembers(g *Group) ([]*User, error) {
	params := &dynamodb.ScanInput{
		TableName: aws.String(c.config.DynamoDBTableGroups),
	}

	// todo - check whether scan is paginated
	result, err := c.client.Scan(params)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Query API call failed: %s", err)
		return nil, err
	}

	groupUsers := []*DynamoDBGroupUser{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &groupUsers)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Unmarshaling dynamodb response failed: %s", err)
		return nil, err
	}

	users := []*User{}
	for _, groupUser := range groupUsers {
		users = append(users, &User{
			Username: groupUser.Username,
		})
	}

	fmt.Println(users)

	return users, nil
}

func (c *dynamoDBClient) GetUsers() ([]*User, error) {
	params := &dynamodb.ScanInput{
		TableName: aws.String(c.config.DynamoDBTableUsers),
	}

	// todo - check whether scan is paginated
	result, err := c.client.Scan(params)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Query API call failed: %s", err)
		return nil, err
	}

	users := []*User{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &users)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Unmarshaling dynamodb response failed: %s", err)
		return nil, err
	}

	return users, nil
}

func (c *dynamoDBClient) AddUserToGroup(u *User, g *Group) error {
	item := map[string]*dynamodb.AttributeValue{
		"groupName": {S: aws.String(g.DisplayName)},
		"username":  {S: aws.String(u.Username)},
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(c.config.DynamoDBTableGroups),
	}

	_, err := c.client.PutItem(input)
	if err != nil {
		log.Error("error calling dynamodb PutItem: %s", err)
		return err
	}

	log.Debug("added user to group in dynamodb: %s", g.DisplayName, u.Username)
	return nil
}

func (c *dynamoDBClient) RemoveUserFromGroup(u *User, g *Group) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"groupName": {
				S: aws.String(g.DisplayName),
			},
			"username": {
				S: aws.String(u.Username),
			},
		},
		TableName: aws.String(c.config.DynamoDBTableGroups),
	}

	_, err := c.client.DeleteItem(input)
	if err != nil {
		log.Error("error calling dynamodb DeleteItem: %s", err)
		return err
	}

	log.Debug("deleted user from group in dynamodb: ", g.DisplayName, u.Username)
	return nil
}

func (c *dynamoDBClient) CreateUser(u *User) error {
	item := map[string]*dynamodb.AttributeValue{
		"username": {S: aws.String(u.Username)},
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(c.config.DynamoDBTableUsers),
	}

	_, err := c.client.PutItem(input)
	if err != nil {
		log.Error("error calling dynamodb PutItem: %s", err)
		return err
	}

	log.Debug("added user to dynamodb: %s", u.Username)
	return nil
}

func (c *dynamoDBClient) DeleteUser(u *User) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"username": {
				S: aws.String(u.Username),
			},
		},
		TableName: aws.String(c.config.DynamoDBTableUsers),
	}

	_, err := c.client.DeleteItem(input)
	if err != nil {
		log.Error("error calling dynamodb DeleteItem: %s", err)
		return err
	}

	log.Debug("deleted user from dynamodb: %s", u.Username)
	return nil
}

func (c *dynamoDBClient) IsUserInGroup(u *User, g *Group) (bool, error) {

	groupMembers, err := c.GetGroupMembers(g)
	if err != nil {
		return false, err
	}

	for _, groupMember := range groupMembers {
		if u.Username == groupMember.Username {
			return true, nil
		}
	}

	return false, nil

}
