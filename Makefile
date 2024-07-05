# Directories // modify as needed
PLUGINS_DIR := ./plugins
INTERMEDIATES_DIR := ./intermediates
OUTPUTS_DIR := ./outputs

# Files // modify as needed
PLUGIN_FILE := $(PLUGINS_DIR)/wordcounter.so
WORDCOUNTER_SRC := ./apps/wordcounter.go
CLIENT_CMD := ./mapreduce/cmd/client/main.go
INPUT_FILES := ./inputs/pg*.txt

# Default target
.PHONY: all
all: run

# Setup target: Creates necessary directories and builds the plugin
.PHONY: setup
setup:
	@ mkdir -p ${PLUGINS_DIR}
	@ mkdir -p ${INTERMEDIATES_DIR}
	@ mkdir -p ${OUTPUTS_DIR}
	
	@ go build -buildmode=plugin -o ${PLUGIN_FILE} ${WORDCOUNTER_SRC}

# Clean target: Removes created directories and files
.PHONY: clean
clean:
	@ rm -rf ${PLUGINS_DIR}
	@ rm -rf ${INTERMEDIATES_DIR}
	@ rm -rf ${OUTPUTS_DIR}

# Run target: Cleans, sets up, and runs the client with the plugin and input files
.PHONY: run
run: clean setup
	@ go run ${CLIENT_CMD} ${PLUGIN_FILE} ${INPUT_FILES}