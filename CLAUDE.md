# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands
- Setup: `./setup.sh` to create venv and install dependencies
- Run: `python -m artifish.agent` to run the main agent
- Tests: `pytest` or `pytest tests/specific_test.py` for single test
- Format: `black .` for code formatting
- Type check: `mypy artifish/`

## Coding Guidelines
- **Style**: PEP 8 compliant, 4-space indentation
- **Imports**: stdlib first, third-party next, local imports last
- **Type Hints**: Required for all function parameters and returns
- **Docstrings**: Google style with Args/Returns sections
- **Error Handling**: Use try/except with specific exceptions and logging
- **Naming**: 
  - Classes: CamelCase
  - Functions/variables: snake_case
  - Private methods: _prefixed
- **Architecture**: Modular with clear separation of concerns

## Project Structure
Artifish is a suite of AI agents for Bluesky built with Google ADK, using Supabase for storage and Memgraph for relationship modeling. Core components include: Agent System, Database Layer, Memory System, and Network Crawler.