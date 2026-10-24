$(OBJ_DIR)/tsan/%.o: %.c
	@mkdir -p $(dir $@)
	@echo "  CC-TSAN $<"
	@$(CC) $(TSAN_CFLAGS) $(CPPFLAGS) -MMD -MP -c $< -o $@

$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(dir $@)
	@echo "  CC      $<"
	@$(CC) $(CFLAGS) $(CPPFLAGS) -MMD -MP -c $< -o $@

$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	@echo "  CXX     $<"
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MMD -MP -c $< -o $@
