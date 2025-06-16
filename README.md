# ❄️ Frost

A blazingly fast, lightweight, Terminal User Interface (TUI) SQL IDE for Snowflake, built with Rust. Navigate your data warehouse, execute queries, and analyze results - all from the comfort of your terminal.

### Core Functionality
- **Full SQL Editor** with syntax highlighting, find/replace, and bracket matching
- **Multi-tab Results** - Run a series of queries and switch between all returned result sets
- **Schema Navigator** - Browse databases, schemas, tables, and columns
- **Role-based Access** - Switch between Snowflake roles without leaving the IDE
- **Smart Query Execution** - Run selected text or query at cursor with `Ctrl+Enter`
- **Large Result Handling** - Smoothly handles tens of millions of returned rows using tile-based storage

### Editor Features
- Syntax highlighting for SQL keywords, strings, numbers, comments
- Bracket matching and auto-indentation
- Find and replace strings within the buffer
- Selection modes (rectangular, line, column)
- Undo/redo
- Session recovery and autosave

### Results Viewer
- Column, row, and rectangular selection modes
- Export to CSV
- Copy selections to clipboard
- Find strings within result sets
- Quck Statistical summaries of selected data
- Null value detection and handling

### DB Navigator
- Full file tree exploerer for all available databases
- Toggle secondary role access
- View database tree by role
- Insert table and columns into the editor
- Find matching strings within the navigator

### Other Features
- **Batch Mode** - output a batch file to automate running sql files
- **Background Schema Crawler** - Keeps schema information up-to-date
- **Configurable Themes** - Customize colors via TOML configuration

## Screenshots

![sc1](https://github.com/user-attachments/assets/32b4c1bb-011d-45ce-9c39-1abbb62e08a9)

![sc2](https://github.com/user-attachments/assets/45bc3961-9f3b-4c5b-ab08-db167002e482)

![sc3](https://github.com/user-attachments/assets/7a76632a-05d3-43d2-baf4-abee588efef3)

### Notes
- This is a personal project; it's rough around the edges, but i'll be polishing it and adding features over time.
- Build with "cargo build --release --bins ; if ($?) { cargo run --release }"
- The application won't launch unless the two binaries ("Frost", and "Frost-Crawler") are in the same directory as "Frost.TOML"
- The application will generate a template TOML file upon first launch, but the program won't run until you configure the ODBC connection string in the TOML file; Example:
  ```toml
  connection_string = """
  Driver=SnowflakeDSIIDriver;
  Server=account.region.snowflakecomputing.com;
  UID=your.email@example.com;
  Authenticator=externalbrowser;
  """
- Requires Snowflake ODBC driver to be installed
- Best viewed with a nerd font such as FiraMono.
- If you come across any rendering glitches, double tap F1 to re-draw the screen.
- This has only been tested in Windows Terminal, though there is some untested Ubuntu support built in.

### Current punchlist:
- Remove console print statements that litter the TUI during DB refresh
- Add autocomplete feature
- Add more buffer motions
