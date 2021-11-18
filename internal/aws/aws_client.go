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
	client         *Client
	dynamoDBClient *DynamoDBClient
}

func NewAWSClient(c Client, d DynamoDBClient) (AWSClient, error) {
	return &awsClient{
		client:         &c,
		dynamoDBClient: &d,
	}, nil
}

// IsUserInGroup will determine if user (u) is in group (g)
func (c *awsClient) IsUserInGroup(u *User, g *Group) (bool, error) {
	panic("not implemented")
}

func (c *awsClient) groupChangeOperation(op OperationType, u *User, g *Group) error {
	panic("not implemented")
}

// AddUserToGroup will add the user specified to the group specified
func (c *awsClient) AddUserToGroup(u *User, g *Group) error {
	panic("not implemented")
}

// RemoveUserFromGroup will remove the user specified from the group specified
func (c *awsClient) RemoveUserFromGroup(u *User, g *Group) error {
	panic("not implemented")
}

// FindUserByEmail will find the user by the email address specified
func (c *awsClient) FindUserByEmail(email string) (*User, error) {
	panic("not implemented")
}

// FindUserByID will find the user by the email address specified
func (c *awsClient) FindUserByID(id string) (*User, error) {
	panic("not implemented")
}

// FindGroupByDisplayName will find the group by its displayname.
func (c *awsClient) FindGroupByDisplayName(name string) (*Group, error) {
	panic("not implemented")
}

// CreateUser will create the user specified
func (c *awsClient) CreateUser(u *User) (*User, error) {
	panic("not implemented")
}

// UpdateUser will update/replace the user specified
func (c *awsClient) UpdateUser(u *User) (*User, error) {
	panic("not implemented")
}

// DeleteUser will remove the current user from the directory
func (c *awsClient) DeleteUser(u *User) error {
	panic("not implemented")
}

// CreateGroup will create a group given
func (c *awsClient) CreateGroup(g *Group) (*Group, error) {
	panic("not implemented")
}

// DeleteGroup will delete the group specified
func (c *awsClient) DeleteGroup(g *Group) error {
	panic("not implemented")
}

// GetGroups will return existing groups
func (c *awsClient) GetGroups() ([]*Group, error) {
	panic("not implemented")
}

// GetGroupMembers will return existing groups
func (c *awsClient) GetGroupMembers(g *Group) ([]*User, error) {
	panic("not implemented")
}

// GetUsers will return existing users
func (c *awsClient) GetUsers() ([]*User, error) {
	panic("not implemented")
}
