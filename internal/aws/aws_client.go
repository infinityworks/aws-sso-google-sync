package aws

type AWSClient interface {
	AddUserToGroup(*User, *Group) error
	CreateGroup(*Group) (*Group, error)
	CreateUser(*User) (*User, error)
	DeleteGroup(*Group) error
	DeleteUser(*User) error
	FindGroupByDisplayName(string) (*Group, error)
	FindUserByEmail(string) (*User, error)
	FindUserByID(string) (*User, error)
	GetUsers() ([]*User, error)
	GetGroupMembers(*Group) ([]*User, error)
	IsUserInGroup(*User, *Group) (bool, error)
	GetGroups() ([]*Group, error)
	UpdateUser(*User) (*User, error)
	RemoveUserFromGroup(*User, *Group) error
}

type awsClient struct {
	client         Client
	dynamoDBClient DynamoDBClient
}

func NewAWSClient(c Client, d DynamoDBClient) (AWSClient, error) {
	return &awsClient{
		client:         c,
		dynamoDBClient: d,
	}, nil
}

// IsUserInGroup will determine if user (u) is in group (g)
func (c *awsClient) IsUserInGroup(u *User, g *Group) (bool, error) {
	return c.dynamoDBClient.IsUserInGroup(u, g)
}

// AddUserToGroup will add the user specified to the group specified
func (c *awsClient) AddUserToGroup(u *User, g *Group) error {

	err := c.client.AddUserToGroup(u, g)
	if err != nil {
		return err
	}

	isUserInDynamoDBGroup, err := c.dynamoDBClient.IsUserInGroup(u, g)

	if !isUserInDynamoDBGroup {
		err = c.dynamoDBClient.AddUserToGroup(u, g)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveUserFromGroup will remove the user specified from the group specified
func (c *awsClient) RemoveUserFromGroup(u *User, g *Group) error {
	err := c.client.RemoveUserFromGroup(u, g)
	if err != nil {
		return err
	}

	err = c.dynamoDBClient.RemoveUserFromGroup(u, g)
	if err != nil {
		return err
	}

	return nil

}

// FindUserByEmail will find the user by the email address specified
func (c *awsClient) FindUserByEmail(email string) (*User, error) {
	return c.client.FindUserByEmail(email)
}

// FindUserByID will find the user by the email address specified
func (c *awsClient) FindUserByID(id string) (*User, error) {
	return c.client.FindUserByID(id)
}

// FindGroupByDisplayName will find the group by its displayname.
func (c *awsClient) FindGroupByDisplayName(name string) (*Group, error) {
	return c.client.FindGroupByDisplayName(name)
}

// CreateUser will create the user specified
func (c *awsClient) CreateUser(u *User) (*User, error) {

	err := c.dynamoDBClient.CreateUser(u)
	if err != nil {
		return nil, err
	}

	newUser, err := c.client.CreateUser(u)
	if err != nil {
		return nil, err
	}

	return newUser, nil
}

// UpdateUser will update/replace the user specified
func (c *awsClient) UpdateUser(u *User) (*User, error) {

	newUser, err := c.client.UpdateUser(u)
	if err != nil {
		return nil, err
	}

	return newUser, nil
}

// DeleteUser will remove the current user from the directory
func (c *awsClient) DeleteUser(u *User) error {

	err := c.client.DeleteUser(u)
	if err != nil {
		return err
	}

	err = c.dynamoDBClient.DeleteUser(u)
	if err != nil {
		return err
	}

	return nil
}

// CreateGroup will create a group given
func (c *awsClient) CreateGroup(g *Group) (*Group, error) {

	newGroup, err := c.client.CreateGroup(g)
	if err != nil {
		return nil, err
	}

	err = c.dynamoDBClient.CreateGroup(newGroup)

	if err != nil {
		return nil, err
	}

	return newGroup, nil
}

// DeleteGroup will delete the group specified
func (c *awsClient) DeleteGroup(g *Group) error {

	err := c.client.DeleteGroup(g)
	if err != nil {
		return err
	}

	err = c.dynamoDBClient.DeleteGroup(g)
	if err != nil {
		return err
	}

	return nil
}

// GetGroups will return existing groups
func (c *awsClient) GetGroups() ([]*Group, error) {

	groups, err := c.dynamoDBClient.GetGroups()
	if err != nil {
		return nil, err
	}

	return groups, nil
}

// GetGroupMembers will return existing groups
func (c *awsClient) GetGroupMembers(g *Group) ([]*User, error) {

	groupMembers, err := c.dynamoDBClient.GetGroupMembers(g)
	if err != nil {
		return nil, err
	}

	awsGroupMembers := []*User{}
	for _, groupMember := range groupMembers {
		awsGroupMember, err := c.client.FindUserByEmail(groupMember.Username)
		if err != nil {
			return nil, err
		}

		awsGroupMembers = append(awsGroupMembers, awsGroupMember)
	}
	return awsGroupMembers, nil
}

// GetUsers will return existing users
func (c *awsClient) GetUsers() ([]*User, error) {

	users, err := c.dynamoDBClient.GetUsers()
	if err != nil {
		return nil, err
	}

	awsUsers := []*User{}
	for _, user := range users {
		awsUser, err := c.client.FindUserByEmail(user.Username)
		if err != nil {
			return nil, err
		}

		awsUsers = append(awsUsers, awsUser)
	}

	return awsUsers, nil
}
