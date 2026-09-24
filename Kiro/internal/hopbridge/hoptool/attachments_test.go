package hoptool

import (
	"strings"
	"testing"

	hoplitert "kiro-proxy/internal/runtime/hoplite"
)

func TestAttachmentStore_ServeReadReturnsImage(t *testing.T) {
	st := NewAttachmentStore()
	att := hoplitert.NewAttachment("image/png", []byte("ABC"))
	st.Put("br_1", []hoplitert.Attachment{att})

	out, handled := st.Serve("br_1", ToolLocalFsRead, []byte(`{"file":"`+att.Path+`"}`))
	if !handled {
		t.Fatal("virtual attachment read must not be forwarded to Claude Code")
	}
	if !strings.Contains(string(out), `"type":"image"`) {
		t.Fatalf("no image block: %s", out)
	}

	if _, handled := st.Serve("br_1", ToolLocalFsRead, []byte(`{"file":"README.md"}`)); handled {
		t.Fatal("real file read must be forwarded")
	}
	if out, handled := st.Serve("br_1", ToolLocalFsRead, []byte(`{"file":"`+hoplitert.AttachmentDir+`/deadbeef.png"}`)); !handled ||
		!strings.Contains(string(out), `"isError":true`) {
		t.Fatalf("missing attachment must be an explicit error: %s", out)
	}
}

func TestAttachmentStore_ListAndDrop(t *testing.T) {
	st := NewAttachmentStore()
	att := hoplitert.NewAttachment("image/png", []byte("ABC"))
	st.Put("br_1", []hoplitert.Attachment{att, att}) // dedup by ID
	if n := len(st.List("br_1")); n != 1 {
		t.Fatalf("want 1 after dedup, got %d", n)
	}
	out, handled := st.Serve("br_1", ToolLocalFsList, []byte(`{"dir":"`+hoplitert.AttachmentDir+`"}`))
	if !handled || !strings.Contains(string(out), att.Path) {
		t.Fatalf("list broke: %v %s", handled, out)
	}
	st.Drop("br_1")
	if len(st.List("br_1")) != 0 {
		t.Fatal("drop failed")
	}
}

func TestIsAttachmentPath(t *testing.T) {
	for _, p := range []string{".hoplite-attachments/x.png", "./.hoplite-attachments/x.png", ".hoplite-attachments"} {
		if !IsAttachmentPath(p) {
			t.Fatalf("%q should be virtual", p)
		}
	}
	for _, p := range []string{"README.md", "src/.hoplite-attachments/x"} {
		if IsAttachmentPath(p) {
			t.Fatalf("%q should be real", p)
		}
	}
}
