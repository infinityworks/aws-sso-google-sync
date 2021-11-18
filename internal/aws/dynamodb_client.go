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
	CreateGroup(*Group) error
	DeleteGroup(*Group) error
	GetGroups() ([]*Group, error)
	GetGroupMembers(*Group) ([]*User, error)
	GetUsers() ([]*User, error)
	AddUserToGroup(*User, *Group) error
	RemoveUserFromGroup(*User, *Group) error
	CreateUser(*User) error
	UpdateUser(*User) error
	DeleteUser(*User) error
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

func (c *dynamoDBClient) CreateGroup(group *Group) error {

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

func (c *dynamoDBClient) DeleteGroup(group *Group) error {

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

	groups := []*Group{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &groups)
	if err != nil {
		// todo - make errors better
		log.Fatalf("Unmarshaling dynamodb response failed: %s", err)
		return nil, err
	}

	return groups, nil
}

func (c *dynamoDBClient) GetGroupMembers(g *Group) ([]*User, error) {

	input := &dynamodb.GetItemInput{
		TableName: aws.String(c.config.DynamoDBTableGroups),
		Key: map[string]*dynamodb.AttributeValue{
			"displayName": {
				S: aws.String(g.DisplayName),
			},
		},
	}

	result, err := c.client.GetItem(input)
	group := Group{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &group)
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}

	users := []*User{}

	// todo - get users objects somehow
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
	listUser := dynamodb.AttributeValue{S: &u.Username}
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
			"displayName": {S: aws.String(g.DisplayName)},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}

func (c *dynamoDBClient) RemoveUserFromGroup(u *User, g *Group) error {

	listUser := dynamodb.AttributeValue{S: &u.Username}
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
			"displayName": {S: aws.String(g.DisplayName)},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}

func (c *dynamoDBClient) CreateUser(u *User) error {
	panic("not implemented")
}

func (c *dynamoDBClient) UpdateUser(u *User) error {
	panic("not implemented")
}

func (c *dynamoDBClient) DeleteUser(u *User) error {
	panic("not implemented")
}
