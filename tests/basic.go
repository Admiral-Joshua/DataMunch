package main

import (
	"fmt"
	"munch"
)

type TestData1 struct {
	Firstname string
	Lastname  string
	Email     string
}

func main() {

	qb := munch.QueryBuilder{}

	query := qb.Table("TEST_TABLE_1")

	query.Where("Count", ">", "10")
	query.AndWhere("Age", ">", "5")
	query.OrWhere("Name", "=", "Admiral Joshua")

	fmt.Println(query.ToSQL())

	iQuery := qb.Table("TEST_TABLE_2")
	iQuery.Insert(TestData1{
		Firstname: "Test",
		Lastname:  "User",
		Email:     "test@test.com",
	})
	fmt.Println(iQuery.ToSQL())

	uQuery := qb.Table("TEST_TABLE_3")
	uQuery.Update(TestData1{
		Firstname: "Test",
	})
	uQuery.Where("email", "=", "test@test.com")
	fmt.Println(uQuery.ToSQL())

	dQuery := qb.Table("TEST_TABLE_2")
	dQuery.Where("Firstname", "=", "Leila")
	dQuery.Delete()
	fmt.Println(dQuery.ToSQL())
}
