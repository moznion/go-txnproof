package txnproof

import "testing"

func TestDefaultClassifier(t *testing.T) {
	cases := []struct {
		query string
		want  StatementKind
	}{
		{"SELECT * FROM users", KindRead},
		{"  select 1", KindRead},
		{"-- comment\nSELECT 1", KindRead},
		{"/* block */ SELECT 1", KindRead},
		{"/* multi\nline */\n-- another\n  SELECT 1", KindRead},
		{"EXPLAIN SELECT 1", KindRead},
		{"INSERT INTO users (id) VALUES (1)", KindWrite},
		{"update users set name = 'a'", KindWrite},
		{"DELETE FROM users", KindWrite},
		{"TRUNCATE users", KindWrite},
		{"CREATE TABLE t (id int)", KindWrite},
		{"CALL some_proc()", KindWrite},
		{"WITH x AS (SELECT 1) SELECT * FROM x", KindRead},
		{"WITH moved AS (DELETE FROM t RETURNING *) INSERT INTO archive SELECT * FROM moved", KindWrite},
		{"with x as (update t set a = 1 returning *) select * from x", KindWrite},
		{"BEGIN", KindBegin},
		{"START TRANSACTION", KindBegin},
		{"COMMIT", KindCommit},
		{"END", KindCommit},
		{"ROLLBACK", KindRollback},
		{"ROLLBACK TO SAVEPOINT sp1", KindOther},
		{"SAVEPOINT sp1", KindOther},
		{"SET search_path TO public", KindOther},
		{"", KindOther},
		{"-- only a comment", KindOther},
	}
	for _, c := range cases {
		if got := DefaultClassifier(c.query); got != c.want {
			t.Errorf("DefaultClassifier(%q) = %v, want %v", c.query, got, c.want)
		}
	}
}
