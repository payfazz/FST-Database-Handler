package data

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Row Per Insertion for Bulk Insert
const (
	rowPerInsert = 100
	aliasConst   = "A"
)

// PostgresRepository is the postgres implementation of generic repository
type PostgresRepository struct {
	db              Queryer
	tableName       string
	elemType        reflect.Type
	selectFields    string
	insertFields    string
	insertParams    string
	updateSetFields string
}

// NewPostgresRepository creates a new generic postgres repository
func NewPostgresRepository(db *sqlx.DB, tableName string, elem interface{}) *PostgresRepository {
	elemType := reflect.TypeOf(elem)
	return &PostgresRepository{
		db:              db,
		tableName:       tableName,
		elemType:        elemType,
		selectFields:    selectFields(elemType),
		insertFields:    insertFields(elemType),
		insertParams:    insertParams(elemType),
		updateSetFields: updateSetFields(elemType),
	}
}

// FindByID finds an element by its id
// it's defined in this project context that
// the element id column in the db should be "id"
func (r *PostgresRepository) FindByID(ctx context.Context, elem interface{}, id interface{}) error {
	err := r.Single(ctx, elem, `"id" = :id`, map[string]interface{}{
		"id": id,
	})
	if err != nil {
		return err
	}

	return nil
}

// SelectAll Select Without where limited by records
func (r *PostgresRepository) SelectAll(ctx context.Context, dest interface{}, orderBy string, limit string, arg interface{}) error {
	db := r.db
	tx, ok := txFromContext(ctx)
	forUpdate := ""
	if ok {
		db = tx
		forUpdate = " FOR UPDATE"
	}

	if orderBy == "" {
		orderBy = "ID"
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`SELECT %s FROM %s ORDER BY %s LIMIT %s %s`,
		r.selectFields, r.tableName, orderBy, limit, forUpdate))
	if err != nil {
		return err
	}

	err = statement.Select(dest, arg)
	if err != nil {
		return err
	}

	return nil
}

// InsertBulkBase insert multiple rows at once
// Using Multiple Prepared Statement to improve performance
// TODO:
// Increase Performance using gopher
func (r *PostgresRepository) InsertBulkBase(ctx context.Context, elem []interface{}) (int, error) {
	count := 0
	// Check if Data Length is zero
	if reflect.Indirect(reflect.ValueOf(elem)).Len() == 0 {
		return count, errors.New("Elem is empty")
	}

	db := r.db
	tx, ok := txFromContext(ctx)
	if ok {
		db = tx
	}

	s := reflect.Indirect(reflect.ValueOf(elem))
	if s.Kind() != reflect.Slice {
		return count, errors.New("elem must be a slice")
	}

	//prepare the statement
	columnLength := 0
	columnName := []string{}
	for i := 0; i < r.elemType.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if dbTag == "created_at" {
			createdAtValue := fmt.Sprintf("%s", "created_at")
			columnName = append(columnName, createdAtValue)
			columnLength++
		}
		if dbTag == "updated_at" {
			updatedAtValue := fmt.Sprintf("%s", "updated_at")
			columnName = append(columnName, updatedAtValue)
			columnLength++
		}
	}

	for i := 0; i < r.elemType.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !emptyTag(dbTag) && !readOnlyTag(dbTag) {
			columnName = append(columnName, fmt.Sprintf("%s", dbTag))
			columnLength++
		}
	}

	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES `, r.tableName, r.insertFields)
	sqlQuery := writeStmt(rowPerInsert, columnLength, stmt)
	query, err := db.Prepare(sqlQuery)
	if err != nil {
		return count, err
	}

	bindValues := []interface{}{}
	createTag := false
	updateTag := false
	for i, column := range elem {
		// Add CreatedAt and UpdatedAt field
		now := time.Now().UTC()
		rows, ok := column.([]interface{})
		if ok {
			for j, row := range rows {
				dbTag := r.elemType.Field(j).Tag.Get("db")
				if !emptyTag(dbTag) && !readOnlyTag(dbTag) {
					if r.elemType.Field(j).Type.Kind() == reflect.Int64 {
						row = StringToInt(fmt.Sprintf("%s", row))
						if err != nil {
							return count, err
						}
					}
					bindValues = append(bindValues, row)
				}
				if createdTag(dbTag) {
					createTag = true
				}
				if updatedTag(dbTag) {
					updateTag = true
				}
			}
		} else {
			s := reflect.Indirect(reflect.ValueOf(column))
			for j := 0; j < r.elemType.NumField(); j++ {
				dbTag := r.elemType.Field(j).Tag.Get("db")
				if !emptyTag(dbTag) && !readOnlyTag(dbTag) {
					bindValues = append(bindValues, reflect.Indirect(s.Field(j)).Interface())
				}
				if createdTag(dbTag) {
					createTag = true
				}
				if updatedTag(dbTag) {
					updateTag = true
				}
			}
		}
		// Created_at
		if createTag {
			bindValues = append(bindValues, now)
		}
		// Updated_at
		if updateTag {
			bindValues = append(bindValues, now)
		}

		if (i+1)%rowPerInsert == 0 {
			//format all vals at once
			res, err := query.Exec(bindValues...)
			if err != nil {
				return count, err
			}
			affectedRowsCount, err := res.RowsAffected()
			if err != nil {
				return count, err
			}
			count = count + int(affectedRowsCount)
			bindValues = nil
		}
	}

	// To Insert data Based on Data Mod RowPerInsert
	// Ex. When There is 4404 data, This part Insert the 404
	// when rowPerInsert is 1000
	if len(bindValues) > 0 {
		sqlQuery := writeStmt((len(bindValues)/columnLength)%rowPerInsert, columnLength, stmt)

		//prepare the statement
		query, err := db.Prepare(sqlQuery)
		if err != nil {
			return count, err
		}
		res, err := query.Exec(bindValues...)
		if err != nil {
			return count, err
		}
		affectedRowsCount, err := res.RowsAffected()
		if err != nil {
			return count, err
		}
		count = count + int(affectedRowsCount)
		bindValues = nil
	}

	return count, nil
}

// InsertBulk Call insertBulkbase without returning row count
func (r *PostgresRepository) InsertBulk(ctx context.Context, elem []interface{}) error {
	_, err := r.InsertBulkBase(ctx, elem)
	return err
}

// InsertBulkWithCount Call insertBulkbase with returning row count
func (r *PostgresRepository) InsertBulkWithCount(ctx context.Context, elem []interface{}) (int, error) {
	return r.InsertBulkBase(ctx, elem)
}

// Insert inserts a new element into the database.
// It assumes the primary key of the table is "id" with serial type.
// It will set the "owner" field of the element with the current account in the context if exists.
// It will set the "createdAt" and "updatedAt" fields with current time.
// If immutable set true, it won't insert the updatedAt
func (r *PostgresRepository) Insert(ctx context.Context, elem interface{}, dest interface{}) error {
	db := r.db
	tx, ok := txFromContext(ctx)
	if ok {
		db = tx
	}

	query := `INSERT INTO %s (%s) VALUES (%s) RETURNING %s`
	query = fmt.Sprintf(query, r.tableName, r.insertFields, r.insertParams, r.selectFields)
	statement, err := db.PrepareNamed(query)
	if err != nil {
		return err
	}

	dbArgs := r.insertArgs(elem)
	err = statement.Get(dest, dbArgs)
	if err != nil {
		return err
	}
	return nil
}

// Single queries an element according to the query & argument provided
// This function should be used only when fetching 1 row of data
func (r *PostgresRepository) Single(ctx context.Context, elem interface{}, where string, arg interface{}) error {
	db := r.db
	tx, ok := txFromContext(ctx)
	forUpdate := ""
	if ok {
		db = tx
		forUpdate = " FOR UPDATE"
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`SELECT %s FROM %s WHERE %s %s LIMIT 1`,
		r.selectFields, r.tableName, where, forUpdate))

	if err != nil {
		return err
	}

	// Return Elem as result row
	err = statement.Get(elem, arg)
	if err != nil {
		return err
	}

	return nil
}

// CustomQuery queries the elements without limitation
func (r *PostgresRepository) CustomQuery(ctx context.Context, stmt string, arg []interface{}) ([]interface{}, error) {
	db := r.db
	tx, ok := txFromContext(ctx)
	forUpdate := ""
	if ok {
		db = tx
		forUpdate = ""
	}

	rows, err := db.Queryx(fmt.Sprintf(`%s%s`, stmt, forUpdate), arg...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Result is your slice string.
	rawResult := make([]interface{}, len(cols))

	payload := make([]interface{}, 0)
	columnPointers := make([]interface{}, len(cols)) // A temporary interface{} slice

	for i := range rawResult {
		columnPointers[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		err := rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make([]interface{}, 0)
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			m = append(m, *val)
		}
		payload = append(payload, m)
	}
	return payload, nil
}

func (r *PostgresRepository) CustomAnyQuery(ctx context.Context, stmt string, arg interface{}) ([]interface{}, error) {
	db := r.db
	tx, ok := txFromContext(ctx)
	forUpdate := ""
	if ok {
		db = tx
		forUpdate = " FOR UPDATE"
	}

	rows, err := db.Queryx(fmt.Sprintf(`%s%s`, stmt, forUpdate), pq.Array(arg))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Result is your slice string.
	rawResult := make([]interface{}, len(cols))

	payload := make([]interface{}, 0)
	columnPointers := make([]interface{}, len(cols)) // A temporary interface{} slice

	for i := range rawResult {
		columnPointers[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		err := rows.Scan(columnPointers...)
		if err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make([]interface{}, 0)
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			m = append(m, *val)
		}
		payload = append(payload, m)
	}
	return payload, nil
}

// Where queries the elements according to the query & argument provided
// This function should be used only when fetching more than 1 row of data
func (r *PostgresRepository) Where(ctx context.Context, dest interface{}, where string, arg interface{}) error {

	db := r.db
	tx, ok := txFromContext(ctx)
	forUpdate := ""
	if ok {
		db = tx
		forUpdate = " FOR UPDATE"
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`SELECT %s FROM %s WHERE %s%s`,
		r.selectFields, r.tableName, where, forUpdate))
	if err != nil {
		return err
	}

	err = statement.Select(dest, arg)
	if err != nil {
		return err
	}

	return nil
}

// Delete deletes the elem from database.
// Delete not really deletes the elem from the db, but it will set the
// "deletedAt" column to current time.
func (r *PostgresRepository) Delete(ctx context.Context, where string, arg interface{}) error {
	db := r.db
	tx, ok := txFromContext(ctx)
	if ok {
		db = tx
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		UPDATE %s SET "deleted_at" = :deleted_at
				WHERE %s`, r.tableName, where))
	if err != nil {
		return err
	}

	_, err = statement.Exec(arg)
	if err != nil {
		return err
	}
	return nil
}

// PermanentDelete Delete data rows From Database (USE WITH CAUTION)
func (r *PostgresRepository) PermanentDelete(ctx context.Context, where string, arg interface{}) error {
	db := r.db
	tx, ok := txFromContext(ctx)
	if ok {
		db = tx
	}

	if arg == nil {
		return errors.New("There Must be Where condition for Deletion Process")
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		DELETE FROM %s WHERE %s`, r.tableName, where))
	if err != nil {
		return err
	}

	_, err = statement.Exec(arg)
	if err != nil {
		return err
	}
	return nil
}

// Update Update records from specific table with specific criteria
func (r *PostgresRepository) Update(ctx context.Context, fields string, where string, arg interface{}) error {
	db := r.db
	tx, ok := txFromContext(ctx)
	if ok {
		db = tx
	}
	alias := aliasConst
	statement, err := db.PrepareNamed(fmt.Sprintf(`
		UPDATE %s %s SET %s
				WHERE %s`, r.tableName, alias, fields, where))
	if err != nil {
		return err
	}

	_, err = statement.Exec(arg)
	if err != nil {
		return err
	}
	return nil
}

func (r *PostgresRepository) insertArgs(elem interface{}) map[string]interface{} {
	res := map[string]interface{}{}
	v := reflect.Indirect(reflect.ValueOf(elem))
	for i := 0; i < r.elemType.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			res[dbTag] = v.Field(i).Interface()
		}
	}

	res["created_at"] = time.Now().UTC()
	res["updated_at"] = time.Now().UTC()
	res["deleted_at"] = nil
	return res
}

// txFromContext returns the trasanction object from the context
func txFromContext(ctx context.Context) (Queryer, bool) {
	q, ok := ctx.Value(TXCONTEXTKEY).(Queryer)
	return q, ok
}

func selectFields(elemType reflect.Type) string {
	dbFields := []string{}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag != "" && dbTag != "-" {
			dbFields = append(dbFields, fmt.Sprintf(`"%s"`, dbTag))
		}
	}
	return strings.Join(dbFields, ", ")
}

func insertFields(elemType reflect.Type) string {
	dbFields := make([]string, 0)
	createTag := false
	updateTag := false
	deleteTag := false

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			dbFields = append(dbFields, fmt.Sprintf(`"%s"`, dbTag))
		}
		if createdTag(dbTag) {
			createTag = true
		}
		if updatedTag(dbTag) {
			updateTag = true
		}
	}
	if createTag {
		dbFields = append(dbFields, `"created_at"`)
	}

	if updateTag {
		dbFields = append(dbFields, `"updated_at"`)
	}

	if deleteTag {
		dbFields = append(dbFields, `"deleted_at"`)
	}

	return strings.Join(dbFields, ", ")
}

func insertParams(elemType reflect.Type) string {
	dbParams := []string{}
	createTag := false
	updateTag := false

	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			dbParams = append(dbParams, fmt.Sprintf(":%s", dbTag))
		}
		if updatedTag(dbTag) {
			updateTag = true
		}
		if createdTag(dbTag) {
			createTag = true
		}
	}
	if createTag {
		dbParams = append(dbParams, ":created_at")
	}
	if updateTag {
		dbParams = append(dbParams, ":updated_at")
	}
	return strings.Join(dbParams, ", ")
}

func updateSetFields(elemType reflect.Type) string {
	setFields := []string{`"updated_at" = :updated_at`}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			setFields = append(setFields, fmt.Sprintf(`"%s" = :%s`, dbTag, dbTag))
		}
	}
	return strings.Join(setFields, ",")
}

func idTag(dbTag string) bool {
	return dbTag == "id"
}

func emptyTag(dbTag string) bool {
	emptyTags := []string{"", "-"}
	for _, t := range emptyTags {
		if dbTag == t {
			return true
		}
	}
	return false
}
func createdTag(dbTag string) bool {
	if dbTag == "created_at" {
		return true
	}
	return false
}
func updatedTag(dbTag string) bool {
	if dbTag == "updated_at" {
		return true
	}
	return false
}

func deleteTag(dbTag string) bool {
	if dbTag == "deleted_at" {
		return true
	}
	return false
}

func readOnlyTag(dbTag string) bool {
	readOnlyTags := []string{
		"is_deleted",
		"deleted_at",
		"id",
		"created_at",
		"updated_at",
		"topup_method_id",
		"bs_topup_banktransfer_id",
		"bs_topup_virtualaccount_id",
		"bs_topup_provider_id",
		"topup_id",
		"topup_recon_matched_id",
		"topup_recon_unmatched_id",
		"statement_id",
		"transfer_recon_unmatched_id",
		"transfer_recon_matched_id",
	}
	for _, t := range readOnlyTags {
		if dbTag == t {
			return true
		}
	}
	return false
}

// WriteStmt function to Write query Statement
func writeStmt(rowPerInsert, numFields int, stmt string) string {
	// Var
	var str strings.Builder

	str.WriteString(stmt)
	for i := 0; i < rowPerInsert; i++ {
		str.WriteString("(")
		for k := 0; k < numFields; k++ {
			if k != numFields-1 {
				str.WriteString(`$` + strconv.Itoa((i*numFields)+k+1) + ",")
			} else {
				str.WriteString(`$` + strconv.Itoa((i*numFields)+k+1))
			}
		}
		str.WriteString("),")
	}
	return strings.TrimSuffix(str.String(), ",")
}

// StringToInt remove .00 space and convert string to int
func StringToInt(value string) int {
	i := strings.Index(value, ".")
	if i > -1 {
		value = value[:i]
	}
	result, _ := strconv.Atoi(strings.TrimSpace(value))
	return result
}
