package munch

import (
	"fmt"
	"testing"
)

var qb = &QueryBuilder{}

/*func TestInitialiseQB(t *testing.T) {
	qb := &QueryBuilder{}

	if qb == nil {
		t.Error("initialisation of query builder interface failed")
	}
}*/

func assertEqual(actual, expected string) error {
	if actual != expected {
		return fmt.Errorf("mismatch!\nexpected:\t %s\ngot:\t\t %s", expected, actual)
	}

	return nil
}

// TestBasicRawSelect
// Expects: SELECT * FROM `TEST_TABLE_1` WHERE `Firstname` = 'Test' AND `Lastname` = 'User' AND `Email` = 'test@test.com';
func TestBasicRawSelect(t *testing.T) {
	query := qb.Table("TEST_TABLE_1")
	query.WhereRaw("Firstname", "=", "Test")
	query.AndWhereRaw("Lastname", "=", "User")
	query.AndWhereRaw("Email", "=", "test@test.com")

	sql := query.ToSQL()

	err := assertEqual(sql, "SELECT * FROM `TEST_TABLE_1` WHERE `Firstname` = 'Test' AND `Lastname` = 'User' AND `Email` = 'test@test.com';")

	if err != nil {
		t.Error(err.Error())
	}
}

// TestWhereOr
// Expects: SELECT * FROM `TEST_TABLE_1` WHERE `Firstname` = 'Test' OR `Email` = 'test@test.com';
func TestWhereOr(t *testing.T) {
	query := qb.Table("TEST_TABLE_1")
	query.Where(BasicTestObject{
		Firstname: "Test",
	})
	query.OrWhereRaw("Email", "=", "test@test.com")

	sql := query.ToSQL()

	err := assertEqual(sql, "SELECT * FROM `TEST_TABLE_1` WHERE `Firstname` = 'Test' OR `Email` = 'test@test.com';")
	if err != nil {
		t.Error(err.Error())
	}
}

type BasicTestObject struct {
	Firstname string
	Lastname  string
	Email     string
}

// TestBasicInsert
// Expects: INSERT INTO `TEST_TABLE_2` (`Firstname`, `Lastname`, `Email`) VALUES ('Test', 'User', 'test@test.com');
func TestBasicInsert(t *testing.T) {
	query := qb.Table("TEST_TABLE_2")
	query.Insert(BasicTestObject{
		Firstname: "Test",
		Lastname:  "User",
		Email:     "test@test.com",
	})

	sql := query.ToSQL()

	err := assertEqual(sql, "INSERT INTO `TEST_TABLE_2` (`Firstname`, `Lastname`, `Email`) VALUES ('Test', 'User', 'test@test.com');")

	if err != nil {
		t.Error(err.Error())
	}
}

// TestBasicUpdate
// Expects: UPDATE `TEST_TABLE_3` SET `Firstname` = 'Test', `Lastname` = 'User' WHERE `Email` = 'test@test.com';
func TestBasicUpdate(t *testing.T) {
	query := qb.Table("TEST_TABLE_3")
	query.Update(BasicTestObject{
		Firstname: "Test",
		Lastname:  "User",
	})
	query.Where(BasicTestObject{
		Email: "test@test.com",
	})

	sql := query.ToSQL()
	err := assertEqual(sql, "UPDATE `TEST_TABLE_3` SET `Firstname` = 'Test', `Lastname` = 'User' WHERE `Email` = 'test@test.com';")

	if err != nil {
		t.Error(err.Error())
	}
}

// TestBasicDelete
// Expects: DELETE FROM `TEST_TABLE_4` WHERE `Email` = 'test@test.com';
func TestBasicDelete(t *testing.T) {
	query := qb.Table("TEST_TABLE_4")
	query.Where(BasicTestObject{Email: "test@test.com"})
	query.Del()

	sql := query.ToSQL()
	err := assertEqual(sql, "DELETE FROM `TEST_TABLE_4` WHERE `Email` = 'test@test.com';")

	if err != nil {
		t.Error(err.Error())
	}
}

type WhereInObject struct {
	Usernames []string `sql:"Username"`
}

// TestWhereInString
// Expects: SELECT * FROM `Users` WHERE `Username` IN ('Test', 'Test2', 'Test3');
func TestWhereInString(t *testing.T) {
	query := qb.Table("Users")
	query.Where(WhereInObject{Usernames: []string{"Test", "Test2", "Test3"}})
	//query.WhereRaw("Username", "IN", []string{"Test","Test2","Test3"})

	sql := query.ToSQL()
	err := assertEqual(sql, "SELECT * FROM `Users` WHERE `Username` IN ('Test', 'Test2', 'Test3');")

	if err != nil {
		t.Error(err.Error())
	}
}

type TestGroupObj struct {
	GID   int    `sql:"GroupId"`
	GName string `sql:"GroupName"`
}

// TestInsertWithTags
// Expects: INSERT INTO `UserGroups` (`GroupId`, `GroupName`) VALUES (5, 'Test Group 5');
func TestInsertWithTags(t *testing.T) {
	query := qb.Table("UserGroups")
	query.Insert(TestGroupObj{
		GID:   5,
		GName: "Test Group 5",
	})

	sql := query.ToSQL()
	err := assertEqual(sql, "INSERT INTO `UserGroups` (`GroupId`, `GroupName`) VALUES (5, 'Test Group 5');")

	if err != nil {
		t.Error(err.Error())
	}
}

// TestWhereInInt
// Expects: SELECT * FROM `UserGroups` WHERE `GroupId` IN (1, 2, 3, 4);
func TestWhereInInt(t *testing.T) {
	query := qb.Table("UserGroups")
	query.WhereIn("GroupId", []int{1, 2, 3, 4}, false)

	sql := query.ToSQL()
	err := assertEqual(sql, "SELECT * FROM `UserGroups` WHERE `GroupId` IN (1, 2, 3, 4);")

	if err != nil {
		t.Error(err.Error())
	}
}

// TestMultipleWhereInOne
// Expects: SELECT * FROM `TEST_TABLE_1` WHERE `Firstname` = 'Test' AND `Lastname` = 'User' AND `Email` = 'test@test.com';
func TestMultipleWhereInOne(t *testing.T) {
	query := qb.Table("TEST_TABLE_1")

	query.Where([]BasicTestObject{
		{Firstname: "Test"},
		{Lastname: "User"},
		{Email: "test@test.com"},
	})

	sql := query.ToSQL()
	err := assertEqual(sql, "SELECT * FROM `TEST_TABLE_1` WHERE `Firstname` = 'Test' AND `Lastname` = 'User' AND `Email` = 'test@test.com';")

	if err != nil {
		t.Error(err.Error())
	}
}

// TestSelectSpecificColumns
// Expects: SELECT `UserId`, `Username` FROM `Users` WHERE `Email` = 'test@test.com';
func TestSelectSpecificColumns(t *testing.T) {
	query := qb.Table("Users")
	query.Where(BasicTestObject{Email: "test@test.com"})
	query.Select([]string{"UserId", "Username"})

	sql := query.ToSQL()
	err := assertEqual(sql, "SELECT `UserId`, `Username` FROM `Users` WHERE `Email` = 'test@test.com';")

	if err != nil {
		t.Error(err.Error())
	}
}

type ComplexTestObj struct {
	MyText  string
	MyFloat float64
	MyInt   int
	MyBool  bool
}

// TestInsertComplexObject
// Expects: INSERT INTO `ComplexTable` (`MyText`, `MyFloat`, `MyInt`, `MyBool`) VALUES ('Test', 5.66, 9, TRUE);
func TestInsertComplexObject(t *testing.T) {
	query := qb.Table("ComplexTable")
	query.Insert(ComplexTestObj{
		MyText:  "Test",
		MyFloat: 5.660,
		MyInt:   9,
		MyBool:  true,
	})

	sql := query.ToSQL()
	err := assertEqual(sql, "INSERT INTO `ComplexTable` (`MyText`, `MyFloat`, `MyInt`, `MyBool`) VALUES ('Test', 5.66, 9, TRUE);")

	if err != nil {
		t.Error(err.Error())
	}
}
