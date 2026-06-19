package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

const BootstrapUserEmail = "platform@oaix.local"

type PlatformUser struct {
	ID          int64      `json:"id"`
	Email       *string    `json:"email,omitempty"`
	DisplayName *string    `json:"display_name,omitempty"`
	Role        string     `json:"role"`
	Status      string     `json:"status"`
	Plan        *string    `json:"plan,omitempty"`
	Notes       *string    `json:"notes,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	LastSeenAt  *time.Time `json:"last_seen_at,omitempty"`
	DisabledAt  *time.Time `json:"disabled_at,omitempty"`
}

type PlatformUserListOptions struct {
	Limit             int
	Offset            int
	Query             string
	Role              string
	Status            string
	Plan              string
	ActiveWithinHours int
	InactiveForHours  int
	Sort              string
}

type PlatformUserPatch struct {
	DisplayName *string
	Role        *string
	Status      *string
	Plan        *string
	Notes       *string
}

type APIKey struct {
	ID         int64      `json:"id"`
	UserID     *int64     `json:"user_id,omitempty"`
	Kind       string     `json:"kind"`
	Name       string     `json:"name"`
	Role       string     `json:"role"`
	KeyPrefix  string     `json:"key_prefix"`
	Scopes     []string   `json:"scopes,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
}

type CreatedAPIKey struct {
	APIKey
	PlaintextKey string `json:"plaintext_key"`
}

type ValidatedAPIKey struct {
	APIKey
	User PlatformUser `json:"user"`
}

func (s *Store) BootstrapUserID(ctx context.Context) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		insert into platform_users(email, display_name, role, status)
		values ($1, 'Platform Bootstrap', 'admin', 'active')
		on conflict (email) do update set updated_at = now()
		returning id
	`, BootstrapUserEmail).Scan(&id)
	return id, err
}

func (s *Store) CreatePlatformUserWithPassword(ctx context.Context, email string, password string, displayName string, role string) (PlatformUser, error) {
	email = normalizeEmail(email)
	if email == "" {
		return PlatformUser{}, fmt.Errorf("email is required")
	}
	if len(password) < 6 {
		return PlatformUser{}, fmt.Errorf("password must be at least 6 characters")
	}
	role = normalizeUserRole(role)
	if role == "" {
		role = "user"
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return PlatformUser{}, err
	}
	var item PlatformUser
	row := s.pool.QueryRow(ctx, `
		insert into platform_users(email, password_hash, display_name, role, status)
		values ($1, $2, nullif($3, ''), $4, 'active')
		returning id, email, display_name, role, status, plan, notes, created_at, updated_at, last_seen_at, disabled_at
	`, email, string(hash), truncate(displayName, 128), role)
	if err := scanPlatformUser(row, &item); err != nil {
		return PlatformUser{}, err
	}
	return item, nil
}

func (s *Store) AuthenticatePlatformUser(ctx context.Context, email string, password string) (PlatformUser, error) {
	email = normalizeEmail(email)
	var item PlatformUser
	var passwordHash *string
	err := s.pool.QueryRow(ctx, `
		update platform_users
		set last_seen_at = now(), updated_at = now()
		where lower(email) = $1
		  and status = 'active'
		returning id, email, display_name, role, status, plan, notes, created_at, updated_at, last_seen_at, disabled_at, password_hash
	`, email).Scan(
		&item.ID, &item.Email, &item.DisplayName, &item.Role, &item.Status, &item.Plan, &item.Notes,
		&item.CreatedAt, &item.UpdatedAt, &item.LastSeenAt, &item.DisabledAt, &passwordHash,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlatformUser{}, pgx.ErrNoRows
	}
	if err != nil {
		return PlatformUser{}, err
	}
	if passwordHash == nil || bcrypt.CompareHashAndPassword([]byte(*passwordHash), []byte(password)) != nil {
		return PlatformUser{}, pgx.ErrNoRows
	}
	return item, nil
}

func (s *Store) GetPlatformUser(ctx context.Context, id int64) (PlatformUser, error) {
	var item PlatformUser
	err := s.pool.QueryRow(ctx, `
		select id, email, display_name, role, status, plan, notes, created_at, updated_at, last_seen_at, disabled_at
		from platform_users
		where id = $1
	`, id).Scan(&item.ID, &item.Email, &item.DisplayName, &item.Role, &item.Status, &item.Plan, &item.Notes, &item.CreatedAt, &item.UpdatedAt, &item.LastSeenAt, &item.DisabledAt)
	return item, err
}

func (s *Store) ListPlatformUsers(ctx context.Context, opts PlatformUserListOptions) ([]PlatformUser, int, error) {
	if opts.Limit <= 0 || opts.Limit > 500 {
		opts.Limit = 100
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	where, args := platformUserWhere(opts)
	var total int
	if err := s.pool.QueryRow(ctx, "select count(*) from platform_users where "+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	orderBy := "id desc"
	switch strings.ToLower(strings.TrimSpace(opts.Sort)) {
	case "email":
		orderBy = "email asc nulls last, id desc"
	case "last_seen":
		orderBy = "last_seen_at desc nulls last, id desc"
	case "created":
		orderBy = "created_at desc, id desc"
	}
	args = append(args, opts.Limit, opts.Offset)
	rows, err := s.pool.Query(ctx, `
		select id, email, display_name, role, status, plan, notes, created_at, updated_at, last_seen_at, disabled_at
		from platform_users
		where `+where+`
		order by `+orderBy+`
		limit $`+fmt.Sprint(len(args)-1)+` offset $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []PlatformUser{}
	for rows.Next() {
		var item PlatformUser
		if err := scanPlatformUser(rows, &item); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func platformUserWhere(opts PlatformUserListOptions) (string, []any) {
	filters := []string{"true"}
	args := []any{}
	arg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}
	if q := strings.TrimSpace(opts.Query); q != "" {
		placeholder := arg("%" + strings.ToLower(q) + "%")
		filters = append(filters, fmt.Sprintf("(lower(coalesce(email, '')) like %s or lower(coalesce(display_name, '')) like %s or id::text = %s)", placeholder, placeholder, arg(q)))
	}
	if role := normalizeUserRole(opts.Role); role != "" && role != "all" {
		filters = append(filters, fmt.Sprintf("role = %s", arg(role)))
	}
	if status := strings.ToLower(strings.TrimSpace(opts.Status)); status != "" && status != "all" {
		filters = append(filters, fmt.Sprintf("status = %s", arg(status)))
	}
	if plan := strings.ToLower(strings.TrimSpace(opts.Plan)); plan != "" && plan != "all" {
		filters = append(filters, fmt.Sprintf("coalesce(plan, '') = %s", arg(plan)))
	}
	if opts.ActiveWithinHours > 0 {
		filters = append(filters, fmt.Sprintf("last_seen_at >= now() - make_interval(hours => %s)", arg(opts.ActiveWithinHours)))
	}
	if opts.InactiveForHours > 0 {
		filters = append(filters, fmt.Sprintf("(last_seen_at is null or last_seen_at < now() - make_interval(hours => %s))", arg(opts.InactiveForHours)))
	}
	return strings.Join(filters, " and "), args
}

func (s *Store) UpdatePlatformUser(ctx context.Context, id int64, patch PlatformUserPatch) (PlatformUser, error) {
	var item PlatformUser
	err := s.pool.QueryRow(ctx, `
		update platform_users
		set display_name = case when $2::boolean then nullif($3, '') else display_name end,
		    role = case when $4::boolean then $5 else role end,
		    status = case when $6::boolean then $7 else status end,
		    plan = case when $8::boolean then nullif($9, '') else plan end,
		    notes = case when $10::boolean then nullif($11, '') else notes end,
		    disabled_at = case
		      when $6::boolean and $7 in ('disabled', 'suspended', 'deleted') then coalesce(disabled_at, now())
		      when $6::boolean and $7 = 'active' then null
		      else disabled_at
		    end,
		    updated_at = now()
		where id = $1
		returning id, email, display_name, role, status, plan, notes, created_at, updated_at, last_seen_at, disabled_at
	`, id,
		patch.DisplayName != nil, stringPtrValue(patch.DisplayName),
		patch.Role != nil, normalizeUserRole(stringPtrValue(patch.Role)),
		patch.Status != nil, normalizeUserStatus(stringPtrValue(patch.Status)),
		patch.Plan != nil, strings.ToLower(strings.TrimSpace(stringPtrValue(patch.Plan))),
		patch.Notes != nil, stringPtrValue(patch.Notes),
	).Scan(&item.ID, &item.Email, &item.DisplayName, &item.Role, &item.Status, &item.Plan, &item.Notes, &item.CreatedAt, &item.UpdatedAt, &item.LastSeenAt, &item.DisabledAt)
	return item, err
}

func (s *Store) CreateAPIKey(ctx context.Context, userID *int64, kind string, name string, role string, createdByUserID *int64) (CreatedAPIKey, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "api-key"
	}
	kind = normalizeAPIKeyKind(kind)
	role = normalizeUserRole(role)
	if role == "" {
		role = "user"
	}
	plain, prefix, err := newPlainAPIKey(kind)
	if err != nil {
		return CreatedAPIKey{}, err
	}
	hash := hashString(plain)
	var item APIKey
	err = s.pool.QueryRow(ctx, `
		insert into api_keys(user_id, kind, name, role, key_prefix, key_hash, created_by_user_id)
		values ($1, $2, $3, $4, $5, $6, $7)
		returning id, user_id, kind, name, role, key_prefix, scopes, created_at, updated_at, last_used_at, expires_at, revoked_at
	`, userID, kind, truncate(name, 128), role, prefix, hash, createdByUserID).Scan(
		&item.ID, &item.UserID, &item.Kind, &item.Name, &item.Role, &item.KeyPrefix, scanJSONStringSlice(&item.Scopes),
		&item.CreatedAt, &item.UpdatedAt, &item.LastUsedAt, &item.ExpiresAt, &item.RevokedAt,
	)
	if err != nil {
		return CreatedAPIKey{}, err
	}
	return CreatedAPIKey{APIKey: item, PlaintextKey: plain}, nil
}

func (s *Store) ListAPIKeys(ctx context.Context, scope ResourceScope, userID *int64) ([]APIKey, error) {
	args := []any{}
	filters := []string{"true"}
	if userID != nil {
		args = append(args, *userID)
		filters = append(filters, fmt.Sprintf("user_id = $%d", len(args)))
	} else if !scope.AllowAll {
		filters = append(filters, scope.ownerFilter("user_id", &args))
	}
	rows, err := s.pool.Query(ctx, `
		select id, user_id, kind, name, role, key_prefix, scopes, created_at, updated_at, last_used_at, expires_at, revoked_at
		from api_keys
		where `+strings.Join(filters, " and ")+`
		order by id desc
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []APIKey{}
	for rows.Next() {
		var item APIKey
		if err := rows.Scan(&item.ID, &item.UserID, &item.Kind, &item.Name, &item.Role, &item.KeyPrefix, scanJSONStringSlice(&item.Scopes), &item.CreatedAt, &item.UpdatedAt, &item.LastUsedAt, &item.ExpiresAt, &item.RevokedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) RevokeAPIKey(ctx context.Context, scope ResourceScope, id int64) error {
	args := []any{id}
	where := "id = $1 and revoked_at is null"
	if !scope.AllowAll {
		where += " and " + scope.ownerFilter("user_id", &args)
	}
	tag, err := s.pool.Exec(ctx, `update api_keys set revoked_at = now(), updated_at = now() where `+where, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ValidateAPIKey(ctx context.Context, plaintext string) (ValidatedAPIKey, bool, error) {
	plaintext = strings.TrimSpace(plaintext)
	if plaintext == "" {
		return ValidatedAPIKey{}, false, nil
	}
	hash := hashString(plaintext)
	var out ValidatedAPIKey
	err := s.pool.QueryRow(ctx, `
		select
		  k.id, k.user_id, k.kind, k.name, k.role, k.key_prefix, k.scopes,
		  k.created_at, k.updated_at, k.last_used_at, k.expires_at, k.revoked_at,
		  u.id, u.email, u.display_name, u.role, u.status, u.plan, u.notes,
		  u.created_at, u.updated_at, u.last_seen_at, u.disabled_at
		from api_keys k
		join platform_users u on k.user_id = u.id
		where k.key_hash = $1
		  and k.revoked_at is null
		  and (k.expires_at is null or k.expires_at > now())
	`, hash).Scan(
		&out.ID, &out.UserID, &out.Kind, &out.Name, &out.Role, &out.KeyPrefix, scanJSONStringSlice(&out.Scopes),
		&out.CreatedAt, &out.UpdatedAt, &out.LastUsedAt, &out.ExpiresAt, &out.RevokedAt,
		&out.User.ID, &out.User.Email, &out.User.DisplayName, &out.User.Role, &out.User.Status, &out.User.Plan, &out.User.Notes,
		&out.User.CreatedAt, &out.User.UpdatedAt, &out.User.LastSeenAt, &out.User.DisabledAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ValidatedAPIKey{}, false, nil
	}
	if err != nil {
		return ValidatedAPIKey{}, false, err
	}
	return out, true, nil
}

func (s *Store) TouchAPIKey(ctx context.Context, id int64) error {
	if id <= 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `update api_keys set last_used_at = now(), updated_at = now() where id = $1`, id)
	return err
}

func (s *Store) CreateRegisteredUser(ctx context.Context, email string, password string, displayName string) (PlatformUser, CreatedAPIKey, error) {
	var user PlatformUser
	var key CreatedAPIKey
	err := s.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		email = normalizeEmail(email)
		if email == "" {
			return fmt.Errorf("email is required")
		}
		if len(password) < 6 {
			return fmt.Errorf("password must be at least 6 characters")
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			insert into platform_users(email, password_hash, display_name, role, status)
			values ($1, $2, nullif($3, ''), 'user', 'active')
			returning id, email, display_name, role, status, plan, notes, created_at, updated_at, last_seen_at, disabled_at
		`, email, string(hash), truncate(displayName, 128)).Scan(
			&user.ID, &user.Email, &user.DisplayName, &user.Role, &user.Status, &user.Plan, &user.Notes,
			&user.CreatedAt, &user.UpdatedAt, &user.LastSeenAt, &user.DisabledAt,
		); err != nil {
			return err
		}
		plain, prefix, err := newPlainAPIKey("user")
		if err != nil {
			return err
		}
		key.PlaintextKey = plain
		if err := tx.QueryRow(ctx, `
			insert into api_keys(user_id, kind, name, role, key_prefix, key_hash, created_by_user_id)
			values ($1, 'user', 'default', 'user', $2, $3, $1)
			returning id, user_id, kind, name, role, key_prefix, scopes, created_at, updated_at, last_used_at, expires_at, revoked_at
		`, user.ID, prefix, hashString(plain)).Scan(
			&key.ID, &key.UserID, &key.Kind, &key.Name, &key.Role, &key.KeyPrefix, scanJSONStringSlice(&key.Scopes),
			&key.CreatedAt, &key.UpdatedAt, &key.LastUsedAt, &key.ExpiresAt, &key.RevokedAt,
		); err != nil {
			return err
		}
		return nil
	})
	return user, key, err
}

func scanPlatformUser(row rowScanner, item *PlatformUser) error {
	return row.Scan(&item.ID, &item.Email, &item.DisplayName, &item.Role, &item.Status, &item.Plan, &item.Notes, &item.CreatedAt, &item.UpdatedAt, &item.LastSeenAt, &item.DisabledAt)
}

func normalizeEmail(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeUserRole(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "service", "admin", "readonly_admin", "user":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func normalizeUserStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "active", "disabled", "suspended", "deleted":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "active"
	}
}

func normalizeAPIKeyKind(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "service") {
		return "service"
	}
	return "user"
}

func newPlainAPIKey(kind string) (plain string, prefix string, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	head := "oaix_user"
	if normalizeAPIKeyKind(kind) == "service" {
		head = "oaix_service"
	}
	plain = head + "_" + hex.EncodeToString(raw)
	if len(plain) > 24 {
		prefix = plain[:24]
	} else {
		prefix = plain
	}
	return plain, prefix, nil
}

type jsonStringSliceScanner struct {
	target *[]string
}

func scanJSONStringSlice(target *[]string) jsonStringSliceScanner {
	return jsonStringSliceScanner{target: target}
}

func (s jsonStringSliceScanner) Scan(src any) error {
	if s.target == nil {
		return nil
	}
	switch value := src.(type) {
	case nil:
		*s.target = nil
	case []byte:
		return json.Unmarshal(value, s.target)
	case string:
		return json.Unmarshal([]byte(value), s.target)
	default:
		return fmt.Errorf("unsupported string slice json value %T", src)
	}
	return nil
}
