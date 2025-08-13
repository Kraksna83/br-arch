# BR-Arch - Chunk-Based File Archiver

BR-Arch is a Python utility designed to organize files into evenly-sized chunks, making it ideal for archiving large collections of files to fixed-size media like DVDs, Blu-rays, or tape storage.

## Features

- Group files into chunks of configurable size (default 20GB)
- Keep files from the same directory together when possible
- Generate HTML, JSON, and CSV catalogues of all files
- Create symlinks to original files for easy access
- Calculate MD5 checksums for data integrity verification
- Mark chunks as "burnt" when they're written to media
- Verify and repair symlinks with a dedicated check command
- Incremental updates - add new files to existing chunk structure
- Reset capability with automatic metadata backup
- Tracked directories - maintain history of all source directories

## Installation

Clone the repository and ensure you have Python 3.6+ installed:

```bash
git clone https://github.com/yourusername/br-arch.git
cd br-arch
```

## Usage

BR-Arch offers several commands through its command-line interface:

### Adding Files to Chunks

```bash
python br-arch.py add /path/to/files -s 20 -m
```

Options:
- `-d, --debug`: Enable debug logging
- `-i, --ignore`: Regex pattern for files to ignore
- `-o, --output`: Set output directory (default: current directory)
- `-m, --md5`: Calculate MD5 hashes for all files
- `-s, --size`: Chunk size in GB (default: 20)

### Marking Chunks as Burnt

Once you've written a chunk to permanent media:

```bash
python br-arch.py burn 1 2 3
```

This marks chunks 1, 2, and 3 as "burnt," preventing further files from being added to them.

### Recalculating Hashes

If you need to verify or update file hashes:

```bash
python br-arch.py rehash /path/to/files
```

### Checking Symlinks

Verify that all expected symlinks exist and are valid:

```bash
python br-arch.py check
```

Add the `-f` flag to automatically fix missing or extra symlinks:

```bash
python br-arch.py check -f
```

### Resetting Everything

To start fresh while keeping a backup of your metadata:

```bash
python br-arch.py reset
```

Options:
- `-y, --yes`: Skip the confirmation prompt
- `-d, --debug`: Enable debug logging

This creates a timestamped backup of your chunks_meta.json file and removes all chunk directories.

### Restoring from a Backup

To recreate your chunk structure from a previous metadata file:

```bash
python br-arch.py restore chunks_meta_20240531_120101.json.bak
```

Options:
- `-y, --yes`: Skip the confirmation prompt
- `-d, --debug`: Enable debug logging

This command will recreate all chunk directories and symlinks based on the specified metadata file, reporting any files that no longer exist on the system.

### Listing Source Directories

To see all directories that have been added to the archive:

```bash
python br-arch.py list
```

Options:
- `-j, --json`: Output in JSON format for scripting
- `-d, --debug`: Enable debug logging

This command displays information about directories that were processed with the 'add' command, including when they were added, how many files they contained, and their total size.

### Rescanning for New Files

To check all previously added directories for new files:

```bash
python br-arch.py rescan
```

Options:
- `-m, --md5`: Calculate MD5 hashes for new files
- `-y, --yes`: Skip the confirmation prompt
- `-d, --debug`: Enable debug logging

This command rescans all directories that were previously added to the archive, looking for new files that weren't included before. It's perfect for incremental updates to maintain your archive over time.

## Examples

### Basic Workflow

1. **Create initial chunks**:
   ```bash
   python br-arch.py add /data/photos -s 4.7 -m
   ```
   This creates chunks of 4.7GB (DVD size) with MD5 hashes.

2. **Write chunks to media**:
   After writing chunks 1-3 to DVDs, mark them as burnt:
   ```bash
   python br-arch.py burn 1 2 3
   ```

3. **Add more files**:
   When you have more files to archive:
   ```bash
   python br-arch.py add /data/more_photos
   ```
   New files will only be added to non-burnt chunks.

4. **Verify symlinks**:
   ```bash
   python br-arch.py check -f
   ```
   This checks and repairs any missing symlinks.

5. **Start fresh if needed**:
   ```bash
   python br-arch.py reset -y
   ```
   This backs up your chunks_meta.json and clears all chunk directories.

6. **Restore from a backup**:
   ```bash
   python br-arch.py restore chunks_meta_20240531_120101.json.bak
   ```
   This recreates your chunk structure from a backup metadata file.

7. **Rescan all source directories for new files**:
   ```bash
   python br-arch.py rescan -m
   ```
   This checks all previously added directories for new files and adds them to your archive.

## How It Works

BR-Arch uses a "best-fit" bin packing algorithm to allocate files to chunks:

1. Files are grouped by directory to keep related files together
2. Directories are sorted by size (largest first)
3. Each directory is assigned to the chunk where it leaves the least remaining space
4. Files from directories too large for a single chunk are assigned individually
5. Symlinks are created in `chunk_N` directories, maintaining original directory structure
6. Catalogues are generated in `chunk_N/_META/` directories

Each chunk directory contains:
- Symlinks to the original files, preserving the directory structure
- A `_META` directory with:
  - `catalogue.json` - JSON format catalogue of all files in all chunks
  - `catalogue.csv` - CSV format catalogue of all files
  - `catalogue.html` - Interactive HTML catalogue with highlighting for files in this chunk

## Output Structure

```
chunks_meta.json          # Metadata about all chunks and files
chunks_meta_20240531_120101.json.bak  # Timestamped backup (created by reset command)
chunk_1/                  # First chunk directory
  _META/                  # Metadata directory for this chunk
    catalogue.json        # JSON catalogue of all files
    catalogue.csv         # CSV catalogue of all files
    catalogue.html        # HTML catalogue with this chunk's files highlighted
  [symlinks to files...]  # Symlinks to original files, maintaining directory structure
chunk_2/                  # Second chunk directory
  ...
```

The `chunks_meta.json` file contains:
- A list of all files with their chunk assignments, sizes, and MD5 hashes
- Information about burnt chunks that won't receive new files
- Size information for each chunk
- History of all source directories that have been added to the archive

## License

[Your license here]