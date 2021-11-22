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

	items, err := c.scanAllItems(c.config.DynamoDBTableGroups)
	if err != nil {
		return nil, fmt.Errorf("dynamodb get groups scan: %w", err)
	}

	var groupUsers []*DynamoDBGroupUser
	err = dynamodbattribute.UnmarshalListOfMaps(items, &groupUsers)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling dynamodb get groups response: %w", err)
	}

	groupNames := map[string]struct{}{}
	for _, groupUser := range groupUsers {
		if _, ok := groupNames[groupUser.GroupName]; !ok {
			groupNames[groupUser.GroupName] = struct{}{}
		}
	}

	groups := []*Group{}
	for groupName := range groupNames {
		groups = append(groups, &Group{
			DisplayName: groupName,
		})
	}
	return groups, nil
}

func (c *dynamoDBClient) GetGroupMembers(g *Group) ([]*User, error) {

	queryInput := &dynamodb.QueryInput{
		TableName: aws.String(c.config.DynamoDBTableGroups),
		KeyConditions: map[string]*dynamodb.Condition{
			"groupName": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(g.DisplayName),
					},
				},
			},
		},
	}

	var items []map[string]*dynamodb.AttributeValue
	err := c.client.QueryPages(queryInput, func(page *dynamodb.QueryOutput, lastPage bool) bool {
		items = append(items, page.Items...)
		return !lastPage
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb groups get group members query: %w", err)
	}

	var groupUsers []*DynamoDBGroupUser
	err = dynamodbattribute.UnmarshalListOfMaps(items, &groupUsers)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling dynamodb get group members response: %w", err)
	}

	users := []*User{}
	for _, groupUser := range groupUsers {
		users = append(users, &User{
			Username: groupUser.Username,
		})
	}

	return users, nil
}

func (c *dynamoDBClient) GetUsers() ([]*User, error) {
	items, err := c.scanAllItems(c.config.DynamoDBTableUsers)
	if err != nil {
		return nil, fmt.Errorf("dynamodb users scan: %w", err)
	}

	users := []*User{}
	err = dynamodbattribute.UnmarshalListOfMaps(items, &users)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling dynamodb get users response: %w", err)
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
		return fmt.Errorf("calling dynamodb PutItem with group user: %w", err)
	}

	log.Debug("added user to group in dynamodb: %s, %s", g.DisplayName, u.Username)
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
		return fmt.Errorf("calling dynamodb DeleteItem with group user: %w", err)
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
		return fmt.Errorf("calling dynamodb PutItem with user: %w", err)
	}

	log.Debug("added user to dynamodb: ", u.Username)
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
		return fmt.Errorf("calling dynamodb DeleteItem with user: %w", err)
	}

	log.Debug("deleted user from dynamodb: ", u.Username)
	return nil
}

func (c *dynamoDBClient) IsUserInGroup(u *User, g *Group) (bool, error) {
	queryInput := &dynamodb.QueryInput{
		TableName: aws.String(c.config.DynamoDBTableGroups),
		KeyConditions: map[string]*dynamodb.Condition{
			"groupName": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(g.DisplayName),
					},
				},
			},
			"username": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(u.Username),
					},
				},
			},
		},
	}

	var items []map[string]*dynamodb.AttributeValue
	err := c.client.QueryPages(queryInput, func(page *dynamodb.QueryOutput, lastPage bool) bool {
		items = append(items, page.Items...)
		return !lastPage
	})
	if err != nil {
		return false, fmt.Errorf("dynamodb groups get group members query: %w", err)
	}

	return len(items) > 0, nil

}

func (c *dynamoDBClient) scanAllItems(tableName string) ([]map[string]*dynamodb.AttributeValue, error) {

	params := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	items := []map[string]*dynamodb.AttributeValue{}
	err := c.client.ScanPages(params, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		items = append(items, page.Items...)
		return !lastPage
	})

	if err != nil {
		return nil, fmt.Errorf("scanning all dynamodb items in table [%s]: %w", tableName, err)
	}

	return items, nil
}
