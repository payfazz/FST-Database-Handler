package data

import "context"

// GenericRepository represents the generic repository
// for the domain models that matches with its data models
type GenericRepository interface {
	FindByID(ctx context.Context, elem interface{}, id interface{}) error
	SelectAll(ctx context.Context, elem interface{}, orderBy string, limit string, arg interface{}) error
	InsertBulk(ctx context.Context, elem []interface{}) error
	InsertBulkWithCount(ctx context.Context, elem []interface{}) (int, error)
	Insert(ctx context.Context, elem interface{}, dest interface{}) error
	CustomQuery(ctx context.Context, stmt string, args []interface{}) ([]interface{}, error)
	CustomAnyQuery(ctx context.Context, stmt string, arg interface{}) ([]interface{}, error)
	Where(ctx context.Context, dest interface{}, where string, args interface{}) error
	Single(ctx context.Context, elem interface{}, where string, args interface{}) error
	Delete(ctx context.Context, where string, args interface{}) error
	Update(ctx context.Context, fields string, where string, arg interface{}) error
	PermanentDelete(ctx context.Context, where string, arg interface{}) error
}
