package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	log "github.com/sirupsen/logrus"
)

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

func NewDynamoDBClient(config *DynamoDBConfig) (DynamoDBClient, error) {
	// todo - check whether any of these functions can return an error
	session := session.Must(session.NewSession())
	client := dynamodb.New(session)

	return &dynamoDBClient{
		client: client,
		config: config,
	}, nil
}

func (c *dynamoDBClient) GetGroupsWithMembers() ([]*Group, error) {

	params := &dynamodb.ScanInput{
		TableName: aws.String(c.config.DynamoDBTable),
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

	item, err := dynamodbattribute.MarshalMap(group)
	if err != nil {
		log.Error("error marshalling new group: %s", err)
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(c.config.DynamoDBTable),
	}

	_, err = c.client.PutItem(input)
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
			"DisplayName": {
				S: aws.String(group.DisplayName),
			},
		},
		TableName: aws.String(c.config.DynamoDBTable),
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

	addSet := (&dynamodb.AttributeValue{}).SetSS(aws.StringSlice([]string{user.Username}))
	update := expression.Add(expression.Name("Members"), expression.Value(addSet))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()

	if err != nil {
		return err
	}

	_, err = c.client.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &c.config.DynamoDBTable,
		Key: map[string]*dynamodb.AttributeValue{
			"DisplayName": {S: aws.String(group.DisplayName)},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}

func (c *dynamoDBClient) RemoveUserFromGroup(group *Group, user *User) error {
	deleteSet := (&dynamodb.AttributeValue{}).SetSS(aws.StringSlice([]string{user.Username}))
	update := expression.Delete(expression.Name("groups"), expression.Value(deleteSet))
	expr, err := expression.NewBuilder().WithUpdate(update).Build()

	if err != nil {
		return err
	}

	_, err = c.client.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &c.config.DynamoDBTable,
		Key: map[string]*dynamodb.AttributeValue{
			"DisplayName": {S: aws.String(group.DisplayName)},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}
