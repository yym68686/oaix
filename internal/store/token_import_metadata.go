package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// Native import metadata is applied in the same transaction as credentials.
// Absent fields preserve existing configuration. A failed binding rolls back
// credential changes too, so an account can never be published without its proxy.
func applyTokenImportMetadata(ctx context.Context, tx pgx.Tx, ownerID int64, token *Token, payload map[string]any) error {
	if remark, ok := payload["remark"].(string); ok {
		if _, err := tx.Exec(ctx, `update codex_tokens set remark=nullif($2,'') where id=$1`, token.ID, remark); err != nil {
			return err
		}
		token.Remark = nil
		if remark != "" {
			token.Remark = &remark
		}
	}
	raw, present := payload["proxy_channel_id"]
	if !present || raw == nil {
		return nil
	}
	var channelID int64
	switch value := raw.(type) {
	case int64:
		channelID = value
	case int:
		channelID = int64(value)
	case float64:
		if value < 0 || value >= 9223372036854775808.0 || value != float64(int64(value)) {
			return errors.New("invalid proxy channel id")
		}
		channelID = int64(value)
	case json.Number:
		var err error
		channelID, err = strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return errors.New("invalid proxy channel id")
		}
	default:
		return errors.New("invalid proxy channel id")
	}
	if channelID < 0 {
		return errors.New("invalid proxy channel id")
	}
	if channelID == 0 {
		_, err := tx.Exec(ctx, `delete from token_proxy_bindings where token_id=$1`, token.ID)
		return err
	}
	var id int64
	if err := tx.QueryRow(ctx, `select id from proxy_channels where id=$1 and owner_user_id=$2 for key share`, channelID, ownerID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("proxy channel does not belong to the account owner")
		}
		return err
	}
	_, err := tx.Exec(ctx, `insert into token_proxy_bindings(token_id,owner_user_id,proxy_channel_id) values($1,$2,$3)
  on conflict(token_id) do update set owner_user_id=excluded.owner_user_id,proxy_channel_id=excluded.proxy_channel_id,updated_at=now()`, token.ID, ownerID, channelID)
	return err
}
