package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	log "github.com/sirupsen/logrus"
)

type DynamoDBClient interface {
	AddGroup(*Group) error
	AddUserToGroup(*Group, *User) error
	DeleteUserFromGroup(*Group, *User) error
	GetGroupsWithMembers() ([]*Group, error)
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
	return nil
}

func (c *dynamoDBClient) AddUserToGroup(group *Group, user *User) error {
	return nil
}

func (c *dynamoDBClient) DeleteUserFromGroup(group *Group, user *User) error {
	return nil
}
