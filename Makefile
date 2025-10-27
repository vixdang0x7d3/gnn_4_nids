-include .env
export

.PHONY: notebooks sync_doc

help:
	@echo ""
	@echo "Available targets:"
	@echo ""
	@echo "  help            Display this help message"
	@echo "  notebooks       Start Marimo notebooks in headless mode"
	@echo "  sync_doc        Sync documentation from DOC_SRC to DOC_DST"
	@echo ""


notebooks:
	uv run marimo edit --headless --watch

sync_doc:
	scripts/sync_doc.sh "$(DOC_SRC)" "$(DOC_DST)"
