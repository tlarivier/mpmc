$(BIN_DIR)/%:
	@mkdir -p $(dir $@)
	@echo "  LD      $@"
	@$(LINK) $(LDFLAGS) $^ -o $@ $(LDLIBS)
