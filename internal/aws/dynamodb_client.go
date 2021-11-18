package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	log "github.com/sirupsen/logrus"
)

type DynamoDBConfig struct {
	DynamoDBTableUsers  string
	DynamoDBTableGroups string
}

type DynamoDBClient interface {
	GetGroupsWithMembers() ([]*Group, error)
	AddGroup(*Group) error
	RemoveGroup(*Group) error
	AddUserToGroup(*Group, *User) error
	RemoveUserFromGroup(*Group, *User) error
}

type dynamoDBClient struct {
	client *dynamodb.DynamoDB
	config *DynamoDBConfig
}

func NewDynamoDBClient(config *DynamoDBConfig) DynamoDBClient {
	// todo - check whether any of these functions can return an error
	session := session.Must(session.NewSession())
	client := dynamodb.New(session)

	return &dynamoDBClient{
		client: client,
		config: config,
	}
}

func (c *dynamoDBClient) GetGroupsWithMembers() ([]*Group, error) {

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

	var groups []*Group
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &groups)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Unmarshaling dynamodb response failed: %s", err)
		return nil, err
	}

	return groups, nil
}

func (c *dynamoDBClient) AddGroup(group *Group) error {

	members := []*dynamodb.AttributeValue{}
	for _, member := range group.Members {
		members = append(members, &dynamodb.AttributeValue{
			S: aws.String(member),
		})
	}
	item := map[string]*dynamodb.AttributeValue{
		"displayName": {S: aws.String(group.DisplayName)},
		"members":     {L: members},
		"schema":      {SS: aws.StringSlice(group.Schemas)},
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

	log.Debug("added group to dynamodb: %s", group.DisplayName)
	return nil
}

func (c *dynamoDBClient) RemoveGroup(group *Group) error {

	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"displayName": {
				S: aws.String(group.DisplayName),
			},
		},
		TableName: aws.String(c.config.DynamoDBTableGroups),
	}

	_, err := c.client.DeleteItem(input)
	if err != nil {
		log.Error("error calling dynamodb DeleteItem: %s", err)
		return err
	}

	log.Debug("deleted group from dynamodb: %s", group.DisplayName)
	return nil
}

func (c *dynamoDBClient) AddUserToGroup(group *Group, user *User) error {

	listUser := dynamodb.AttributeValue{S: &user.Username}
	addSet := (&dynamodb.AttributeValue{}).SetL([]*dynamodb.AttributeValue{
		&listUser,
	},
	)

	update := expression.Set(expression.Name("members"), expression.Name("members").ListAppend(expression.Value(addSet)))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()

	if err != nil {
		return err
	}

	_, err = c.client.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &c.config.DynamoDBTableGroups,
		Key: map[string]*dynamodb.AttributeValue{
			"displayName": {S: aws.String(group.DisplayName)},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}

func (c *dynamoDBClient) RemoveUserFromGroup(group *Group, user *User) error {

	listUser := dynamodb.AttributeValue{S: &user.Username}
	deleteSet := (&dynamodb.AttributeValue{}).SetL([]*dynamodb.AttributeValue{
		&listUser,
	},
	)

	update := expression.Delete(expression.Name("members"), expression.Value(deleteSet))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()

	if err != nil {
		return err
	}

	_, err = c.client.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &c.config.DynamoDBTableGroups,
		Key: map[string]*dynamodb.AttributeValue{
			"displayName": {S: aws.String(group.DisplayName)},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}
