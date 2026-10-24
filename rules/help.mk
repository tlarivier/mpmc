help: ## Show this help
	@awk 'BEGIN{FS=":.*## "} /^[a-zA-Z0-9_-]+:.*## /{printf "  %-16s %s\n",$$1,$$2}' $(MAKEFILE_LIST)

.PHONY: help
