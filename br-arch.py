#!/usr/bin/env python3
import os
import sys
import argparse
import re
import json
import csv
import hashlib
import logging
import datetime




args = None

def setup_parser():
    """Create and return the command line argument parser"""
    parser = argparse.ArgumentParser(description="Chunk files into evenly sized groups.")
    
    # Add global arguments that apply to all commands
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    
    # Add subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # 'add' command - the current behavior
    add_parser = subparsers.add_parser('add', help='Add files to chunks')
    add_parser.add_argument("directory", help="Directory to process")
    add_parser.add_argument("-i", "--ignore", default="", help="Regex pattern for files to ignore")
    add_parser.add_argument("-o", "--output", default=".", help="Output directory")
    add_parser.add_argument("-m", "--md5", action="store_true", help="Do MD5 hashes for all files")
    add_parser.add_argument("-s", "--size", type=int, default=20, help="Chunk size in GB (default: 20)")
    
    # 'burn' command to mark chunks as burnt
    burn_parser = subparsers.add_parser('burn', help='Mark chunks as burnt')
    burn_parser.add_argument("chunks", type=int, nargs='+', help="Chunk numbers to mark as burnt")
    
    # 'rehash' command to recalculate hashes
    rehash_parser = subparsers.add_parser('rehash', help='Recalculate hashes for existing files')
    rehash_parser.add_argument("directory", help="Directory to process")
    
    # 'check' command to verify symlinks
    check_parser = subparsers.add_parser('check', help='Verify symlinks in chunk directories')
    check_parser.add_argument("-f", "--fix", action="store_true", help="Fix missing symlinks")
    
    # 'reset' command to remove chunks directories and reset metadata
    reset_parser = subparsers.add_parser('reset', help='Reset by removing all chunks and metadata')
    reset_parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt")
    
    # 'restore' command to recreate chunks from backup metadata
    restore_parser = subparsers.add_parser('restore', help='Restore chunks from a metadata file')
    restore_parser.add_argument("metafile", help="Path to chunks_meta.json file to restore from")
    restore_parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt")
    
    # 'list' command to show tracked directories
    list_parser = subparsers.add_parser('list', help='List tracked directories and their information')
    list_parser.add_argument("-j", "--json", action="store_true", help="Output in JSON format")
    
    # 'rescan' command to reprocess all previously added directories
    rescan_parser = subparsers.add_parser('rescan', help='Rescan all previously added directories for new files')
    rescan_parser.add_argument("-m", "--md5", action="store_true", help="Do MD5 hashes for all files")
    rescan_parser.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompt")

    # 'iso' command to produce command to make iso image from existing chunks
    iso_parser = subparsers.add_parser('iso', help='Show command suggestion to make an ISO image')
    iso_parser.add_argument("chunk_no", nargs='?', default=-1, type=int, help="Number of chunk to produce a command from (optional)")

    return parser

def main():
    """
    Main entry point for the br-arch script.
    
    Parses command line arguments and dispatches to the appropriate command handler.
    Configures logging level based on debug flag and sets global variables needed
    by command handlers.
    """
    global args
    parser = setup_parser()
    
    # If no args or just help requested, print usage and exit
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] in ['-h', '--help']):
        parser.print_help()
        sys.exit(1)
        
    args = parser.parse_args()
    
    # Configure logging based on debug flag
    if hasattr(args, 'debug') and args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.debug("Debug mode enabled")
        logging.debug(f"Arguments: {args}")
        logging.debug(f"Python version: {sys.version}")
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Process based on command
    if args.command == 'add':
        global CHUNK_SIZE, ignore_regex
        CHUNK_SIZE = args.size * 1024**3  # default = 20GB
        ignore_regex = re.compile(args.ignore) if args.ignore else None
        add_new_data(args)
    elif args.command == 'burn':
        burn_chunks(args)
    elif args.command == 'rehash':
        rehash_files(args)
    elif args.command == 'check':
        check_symlinks(args)
    elif args.command == 'reset':
        reset_environment(args)
    elif args.command == 'restore':
        restore_from_metadata(args)
    elif args.command == 'list':
        list_tracked_directories(args)
    elif args.command == 'rescan':
        rescan_directories(args)
    elif args.command == 'iso':
        output_iso_command(args)
    else:
        parser.print_help()
        sys.exit(1)


def add_new_data(args):
    """
    Add new files to chunks, either by creating new chunks or adding to existing ones.
    
    This is the main handler for the 'add' command. It:
    1. Scans the specified directory for files
    2. Loads existing chunks and metadata if available
    3. Identifies new files not already processed
    4. Calculates optimal chunk assignments for new files
    5. Creates symlinks and generates catalogues
    6. Saves updated metadata
    
    Args:
        args: Command line arguments containing directory to process and other options
    """
    # Process the specified directory and calculate total file size
    top_dir = args.directory
    abs_top_dir = os.path.abspath(top_dir)
    # Record the timestamp when this directory was added
    import datetime
    
    timestamp = datetime.datetime.now().isoformat()
    
    # Check for existing metadata file
    preexisting = os.path.join(os.getcwd(), "chunks_meta.json")
    
    # Check if the directory is a subdirectory of already tracked directories
    if os.path.exists(preexisting):
        try:
            with open(preexisting, "r") as f:
                chunks_meta = json.load(f)
            tracked_directories = chunks_meta.get("tracked_directories", [])
            
            for dir_info in tracked_directories:
                existing_dir = dir_info.get('directory', '')
                if existing_dir and (abs_top_dir.startswith(existing_dir + os.sep) or abs_top_dir == existing_dir):
                    logging.error(f"Error: {abs_top_dir} is a subdirectory of (or same as) already tracked directory: {existing_dir}")
                    logging.error("Use 'rescan' command to update the archive with new files from existing tracked directories.")
                    return
        except Exception as e:
            logging.error(f"Error checking tracked directories: {e}")
    
    # Use directory-specific ignore pattern
    ignore_pattern = args.ignore if hasattr(args, 'ignore') else ""
    global ignore_regex
    ignore_regex = re.compile(ignore_pattern) if ignore_pattern else None
    
    files = traverse_with_listdir(top_dir, ignore_regex)
    total_size = sum(size for _, size in files)
    logging.info(f"Total size of files: {total_size / (1024**3):.2f} GB")
    
    # Check for existing metadata file
    preexisting = os.path.join(os.getcwd(), "chunks_meta.json")
    existing_chunks = []
    
    # Load existing metadata if available
    if os.path.exists(preexisting):
        with open(preexisting, "r") as f:
            chunks_meta = json.load(f)
        # Extract metadata components
        burnt_chunks = chunks_meta.get("burnt_chunks", [])
        known_files = chunks_meta.get("known_files", [])
        # Get the tracked directories or initialize an empty list
        tracked_directories = chunks_meta.get("tracked_directories", [])
        directory_ignore_patterns = chunks_meta.get("directory_ignore_patterns", {})
        
        # Use the same chunk size as previous runs for consistency
        args.size = chunks_meta.get("chunk_size_setting", args.size)
        logging.info(f"Using existing chunk size setting: {args.size} GB as it was used previously. If you need to change it, backup and edit your chunks_meta.json file.")
        
        # Reconstruct existing chunks from metadata
        chunk_sizes = chunks_meta.get("chunk_sizes", {})
        existing_chunks = []
        for chunk_id, size_str in chunk_sizes.items():
            chunk_id = int(chunk_id)
            # Make sure files in existing chunks are consistently stored as tuples
            files_in_chunk = []
            for file_path in [f[0] for f in known_files if f[1] == chunk_id]:
                # Find size from known_files
                size = next((f[3] for f in known_files if f[0] == file_path), 0)
                files_in_chunk.append((file_path, size))
            
            size_mb = float(size_str.split()[0])
            size_bytes = int(size_mb * 1024**2)
            existing_chunks.append((files_in_chunk, size_bytes))
        
        logging.info(f"Loaded {len(existing_chunks)} existing chunks")
        logging.info(f"Burnt chunks: {burnt_chunks}")
    else:
        # No existing metadata, start fresh
        burnt_chunks = []
        known_files = []
        tracked_directories = []
        directory_ignore_patterns = {}
    
    # Add the current directory to the tracked directories list
    abs_dir_path = os.path.abspath(top_dir)
    tracked_directories.append({
        "directory": abs_dir_path,
        "timestamp": timestamp,
        "ignore_pattern": ignore_pattern,
        "files_count": len(files),
        "total_size_bytes": total_size,
        "total_size_gb": total_size / (1024**3)
    })
    
    # Store directory-specific ignore pattern
    directory_ignore_patterns[abs_dir_path] = ignore_pattern
    logging.info(f"Added {top_dir} to tracked directories with ignore pattern: '{ignore_pattern}'")
    
    # Identify new files that haven't been processed before
    known_file_paths = {f[0] for f in known_files}
    new_files = [f for f in files if f[0] not in known_file_paths]
    logging.info(f"Found {len(new_files)} new files to process")
    
    # Process the files - either add to existing chunks or create new chunks
    if new_files:
        if existing_chunks:
            # Add new files to existing chunks where possible
            chunks, file_to_chunk = calculate_chunks(
                new_files, burnt_chunks, CHUNK_SIZE, existing_chunks)
            logging.info(f"Added new files to existing chunks, now have {len(chunks)} chunks")
        else:
            # Create new chunks for all files
            chunks, file_to_chunk = calculate_chunks(
                new_files, burnt_chunks, CHUNK_SIZE)
            logging.info(f"Created {len(chunks)} new chunks")
    else:
        # No new files, keep existing chunks as-is
        chunks = existing_chunks
        file_to_chunk = {}
        # Rebuild file-to-chunk mapping for existing files
        for chunk_idx, (chunk_files, _) in enumerate(chunks, start=1):
            for file_path in chunk_files:
                file_to_chunk[file_path] = chunk_idx
    
    # Create symlinks and catalogues for all chunks
    all_new_files = process_chunks(chunks, files, file_to_chunk, known_files)
    all_files = all_new_files
    
    # Calculate sizes of each chunk for reporting
    chunk_sizes = {}
    for path, chunk_number, _, size in all_files:
        if chunk_number is not None:
            if chunk_number not in chunk_sizes:
                chunk_sizes[chunk_number] = 0
            chunk_sizes[chunk_number] += size
    
    # Prepare metadata for saving
    chunks_meta = {
        "known_files": all_files,
        "burnt_chunks": burnt_chunks,
        "chunk_sizes": {str(chunk): f"{size/(1024**2):.1f} MB" for chunk, size in chunk_sizes.items()},
        "chunk_size_setting": args.size,
        "tracked_directories": tracked_directories,
        "directory_ignore_patterns": directory_ignore_patterns
    }
    # Write metadata to disk
    with open("chunks_meta.json", "w") as f:
        json.dump(chunks_meta, f, indent=2)
    
    # Log chunk sizes for information
    for chunk, size in sorted(chunk_sizes.items()):
        logging.info(f"Chunk {chunk}: {size/(1024**3):.2f} GB")
    
    logging.info(f"Finished creating chunks_meta.json with {len(chunks)} chunks.")

def burn_chunks(args):
    """
    Mark specific chunks as "burnt" so they won't receive new files.
    
    This is useful after writing chunks to permanent media like DVDs or tapes.
    Burnt chunks are skipped during file allocation when adding new files.
    
    Args:
        args: Command line arguments containing list of chunk numbers to mark as burnt
    """
    chunk_numbers = args.chunks
    logging.info(f"Marking chunks as burnt: {chunk_numbers}")
    
    preexisting = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(preexisting):
        logging.error("No chunks_meta.json found. Run 'add' command first.")
        sys.exit(1)
        
    with open(preexisting, "r") as f:
        chunks_meta = json.load(f)
        
    burnt_chunks = chunks_meta.get("burnt_chunks", [])
    known_files = chunks_meta.get("known_files", [])
    
    for chunk in chunk_numbers:
        if chunk not in burnt_chunks:
            burnt_chunks.append(chunk)
    
    chunks_meta["burnt_chunks"] = burnt_chunks
    with open("chunks_meta.json", "w") as f:
        json.dump(chunks_meta, f, indent=2)
    
    logging.info(f"Updated burnt chunks: {burnt_chunks}")

def rehash_files(args):
    """
    Recalculate MD5 hashes for files in the archive.
    
    This command is useful when:
    - You want to verify file integrity after some time
    - You previously ran without MD5 calculation and want to add hashes
    - You suspect some files may have changed
    
    Args:
        args: Command line arguments containing the directory to process
    """
    directory = args.directory
    logging.info(f"Recalculating hashes for files in: {directory}")
    
    preexisting = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(preexisting):
        logging.error("No chunks_meta.json found. Run 'add' command first.")
        sys.exit(1)
        
    with open(preexisting, "r") as f:
        chunks_meta = json.load(f)
    
    burnt_chunks = chunks_meta.get("burnt_chunks", [])
    known_files = chunks_meta.get("known_files", [])
    
    updated_files = []
    changed_files = []
    
    for file_entry in known_files:
        path, chunk_number, old_md5, size = file_entry
        
        if not os.path.exists(path):
            logging.warning(f"File not found: {path}")
            updated_files.append(file_entry)
            continue
            
        current_size = os.path.getsize(path)
        
        if current_size != size or not old_md5 or old_md5 == "error":
            try:
                with open(path, "rb") as f:
                    new_md5 = hashlib.md5(f.read()).hexdigest()
                
                if old_md5 and old_md5 != "error" and new_md5 != old_md5:
                    changed_files.append((path, old_md5, new_md5))
                    logging.info(f"Hash changed for {path}: {old_md5} -> {new_md5}")
                
                updated_files.append((path, chunk_number, new_md5, current_size))
            except Exception as e:
                logging.error(f"Error calculating hash for {path}: {e}")
                updated_files.append((path, chunk_number, "error", current_size))
        else:
            updated_files.append(file_entry)
    
    chunks_meta["known_files"] = updated_files
    with open("chunks_meta.json", "w") as f:
        json.dump(chunks_meta, f, indent=2)
    
    logging.info(f"Updated hashes for {len(updated_files)} files")
    logging.info(f"Files with changed hashes: {len(changed_files)}")

def check_symlinks(args):
    """
    Check and optionally fix symlink integrity in chunk directories.
    
    This command verifies that:
    1. All expected symlinks exist in the chunk directories
    2. There are no unexpected extra symlinks
    3. All existing symlinks point to valid files
    
    If the --fix flag is provided, it will:
    - Create missing symlinks
    - Remove extra symlinks that don't correspond to files in the metadata
    
    Args:
        args: Command line arguments with optional 'fix' flag
    """
    logging.info("Checking symlink integrity...")
    
    # Load existing metadata
    preexisting = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(preexisting):
        logging.error("No chunks_meta.json found. Run 'add' command first.")
        sys.exit(1)
        
    with open(preexisting, "r") as f:
        chunks_meta = json.load(f)
    
    known_files = chunks_meta.get("known_files", [])
    burnt_chunks = chunks_meta.get("burnt_chunks", [])
    
    # Group files by chunk number
    files_by_chunk = {}
    for path, chunk_number, _, _ in known_files:
        if chunk_number not in files_by_chunk:
            files_by_chunk[chunk_number] = []
        files_by_chunk[chunk_number].append(path)
    
    # Track issues
    missing_symlinks = []
    extra_symlinks = []
    broken_symlinks = []
    
    # Set up progress tracking
    total_chunks = len(files_by_chunk)
    chunks_processed = 0
    total_files = sum(len(files) for files in files_by_chunk.values())
    files_processed = 0
    
    # Check each chunk directory
    for chunk_number, files in files_by_chunk.items():
        chunks_processed += 1
        progress_percent = (chunks_processed / total_chunks) * 100
        sys.stdout.write(f"\rChecking chunks: {chunks_processed}/{total_chunks} ({progress_percent:.1f}%) - Files: {files_processed}/{total_files}")
        sys.stdout.flush()
        
        if chunk_number in burnt_chunks:
            logging.info(f"Skipping burnt chunk {chunk_number}")
            continue
            
        chunk_dir = f"chunk_{chunk_number}"
        if not os.path.exists(chunk_dir):
            logging.warning(f"Chunk directory not found: {chunk_dir}")
            missing_symlinks.extend([(f, chunk_number) for f in files])
            continue
        
        # Check existing symlinks for this chunk
        expected_symlinks = set()
        for file_path in files:
            files_processed += 1
            #print (f"Processing file: {file_path}")
            # Update progress on every 100 files
            if files_processed % 100 == 0:
                progress_percent = (files_processed / total_files) * 100
                sys.stdout.write(f"\rChecking chunks: {chunks_processed}/{total_chunks} ({(chunks_processed / total_chunks) * 100:.1f}%) - Files: {files_processed}/{total_files} ({progress_percent:.1f}%)")
                sys.stdout.flush()
                
            base_dir = args.directory if hasattr(args, 'directory') else get_base_directory()
            rel_path = os.path.basename(file_path) if not base_dir else os.path.relpath(file_path, base_dir)
            symlink_path = os.path.join(chunk_dir, rel_path)
            expected_symlinks.add(symlink_path)
            
            # Check if symlink exists
            if not os.path.exists(symlink_path):
                missing_symlinks.append((file_path, chunk_number))
            elif not os.path.islink(symlink_path):
                logging.warning(f"Not a symlink: {symlink_path}")
            elif not os.path.exists(os.readlink(symlink_path)):
                # Symlink exists but points to nonexistent file
                broken_symlinks.append((symlink_path, os.readlink(symlink_path)))
        
        # Find extra symlinks in this chunk directory
        for root, dirs, files in os.walk(chunk_dir):
            # Skip _META directory
            if os.path.basename(root) == "_META":
                continue
                
            for file_name in files:
                symlink_path = os.path.join(root, file_name)
                if os.path.islink(symlink_path) and symlink_path not in expected_symlinks:
                    target = os.readlink(symlink_path)
                    extra_symlinks.append((symlink_path, target))
    
    # Finish progress display with newline
    sys.stdout.write("\n")
    sys.stdout.flush()
    
    # Report findings
    if not missing_symlinks and not extra_symlinks and not broken_symlinks:
        logging.info("All symlinks are correct!")
        return
    
    if missing_symlinks:
        logging.warning(f"Found {len(missing_symlinks)} missing symlinks:")
        for file_path, chunk_number in missing_symlinks[:10]:  # Show first 10
            logging.warning(f"  Missing symlink for {file_path} in chunk_{chunk_number}")
        if len(missing_symlinks) > 10:
            logging.warning(f"  ... and {len(missing_symlinks) - 10} more")
    
    if extra_symlinks:
        logging.warning(f"Found {len(extra_symlinks)} extra symlinks:")
        for symlink_path, target in extra_symlinks[:10]:  # Show first 10
            logging.warning(f"  Extra symlink: {symlink_path} -> {target}")
        if len(extra_symlinks) > 10:
            logging.warning(f"  ... and {len(extra_symlinks) - 10} more")
    
    if broken_symlinks:
        logging.warning(f"Found {len(broken_symlinks)} broken symlinks:")
        for symlink_path, target in broken_symlinks[:10]:  # Show first 10
            logging.warning(f"  Broken symlink: {symlink_path} -> {target}")
        if len(broken_symlinks) > 10:
            logging.warning(f"  ... and {len(broken_symlinks) - 10} more")
    
    # Fix missing symlinks if requested
    if hasattr(args, 'fix') and args.fix and missing_symlinks:
        logging.info(f"Fixing {len(missing_symlinks)} missing symlinks...")
        
        for file_path, chunk_number in missing_symlinks:
            chunk_dir = f"chunk_{chunk_number}"
            base_dir = args.directory if hasattr(args, 'directory') else ''
            rel_path = os.path.basename(file_path) if not base_dir else os.path.relpath(file_path, base_dir)
            symlink_path = os.path.join(chunk_dir, rel_path)
            
            # Create parent directories if needed
            os.makedirs(os.path.dirname(symlink_path), exist_ok=True)
            
            # Create symlink
            try:
                os.symlink(file_path, symlink_path)
                logging.info(f"Created symlink: {symlink_path} -> {file_path}")
            except FileExistsError:
                logging.warning(f"Symlink already exists: {symlink_path}")
            except Exception as e:
                logging.error(f"Failed to create symlink {symlink_path}: {e}")
        
        logging.info("Finished fixing missing symlinks")
    
    # Offer to remove extra symlinks
    if hasattr(args, 'fix') and args.fix and extra_symlinks:
        logging.info(f"Removing {len(extra_symlinks)} extra symlinks...")
        
        for symlink_path, _ in extra_symlinks:
            try:
                os.remove(symlink_path)
                logging.info(f"Removed extra symlink: {symlink_path}")
            except Exception as e:
                logging.error(f"Failed to remove symlink {symlink_path}: {e}")
        
        logging.info("Finished removing extra symlinks")


def reset_environment(args):
    """Reset the environment by backing up chunks_meta.json and removing all chunk directories"""
    logging.info("Preparing to reset the environment...")
    
    # Check if chunks_meta.json exists
    meta_file = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(meta_file):
        logging.warning("No chunks_meta.json found. Nothing to reset.")
        return
    
    # Create backup with timestamp
    import datetime
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"chunks_meta_{timestamp}.json.bak"
    
    try:
        # Copy metadata file to backup
        import shutil
        shutil.copy2(meta_file, backup_file)
        logging.info(f"Created backup of chunks_meta.json as {backup_file}")
        
        # Confirm before proceeding unless --yes flag is used
        if not hasattr(args, 'yes') or not args.yes:
            confirm = input("This will remove all chunk directories and reset metadata. Continue? (y/N): ")
            if confirm.lower() != 'y':
                logging.info("Reset operation cancelled.")
                return
        
        # Find and remove all chunk directories
        chunk_dirs = []
        for item in os.listdir(os.getcwd()):
            if os.path.isdir(item) and item.startswith("chunk_"):
                chunk_dirs.append(item)
        
        # Remove chunk directories
        for chunk_dir in chunk_dirs:
            try:
                shutil.rmtree(chunk_dir)
                logging.info(f"Removed directory: {chunk_dir}")
            except Exception as e:
                logging.error(f"Error removing directory {chunk_dir}: {e}")
        
        # Remove the metadata file
        os.remove(meta_file)
        logging.info(f"Removed file: {meta_file}")
        
        logging.info(f"Reset complete. Removed {len(chunk_dirs)} chunk directories.")
        logging.info(f"You can find your backup at: {backup_file}")
        
    except Exception as e:
        logging.error(f"Error during reset operation: {e}")



def process_chunks(chunks, files, file_to_chunk, known_files=None):
    """
    Process the chunks by creating symlinks and catalogues for each chunk.
    
    Args:
        chunks: List of tuples (chunk_files, size) where chunk_files is a list of file paths
        files: List of tuples (file_path, size) for all files
        file_to_chunk: Dictionary mapping file paths to their assigned chunk numbers
        known_files: Previously processed files from existing metadata (optional)
    
    Returns:
        List of tuples (file_path, chunk_number, md5_hash, size) for all processed files
    """
    # Initialize with existing known files or empty list
    all_files = list(known_files) if known_files else []
    processed_chunks = set()
    processed_paths = set(f[0] for f in all_files)
    
    # Iterate through each chunk and process its files
    for idx, (chunk_files, _) in enumerate(chunks, start=1):
        # Skip empty chunks
        if not chunk_files:
            continue
            
        # Create chunk directory if it doesn't exist
        dname = f"chunk_{idx}"
        os.makedirs(dname, exist_ok=True)
        
        # Create metadata directory within the chunk directory
        meta_dir = os.path.join(dname, "_META")
        os.makedirs(meta_dir, exist_ok=True)
        
        # Track files in this chunk for reporting
        chunk_file_list = []
        for file_item in chunk_files:
            # Extract path from tuple if needed
            path = file_item[0] if isinstance(file_item, tuple) else file_item
            
            # Find full file entry from input files list
            file_entry = next((f for f in files if f[0] == path), None)
            if file_entry:
                p, size = file_entry
                try:
                    # Initialize MD5 hash as empty string
                    md5_hash = ""
                    
                    # Calculate MD5 hash if requested in args
                    if hasattr(args, 'md5') and args.md5:
                        # Try to reuse existing MD5 hash from previous run if available
                        if os.path.exists(os.path.join(meta_dir, "catalogue.json")):
                            try:
                                with open(os.path.join(meta_dir, "catalogue.json"), "r") as f:
                                    existing_data = json.load(f)
                                    existing_file = next((item for item in existing_data 
                                                         if item["path"] == p), None)
                                    if existing_file and existing_file.get("md5"):
                                        md5_hash = existing_file["md5"]
                            except Exception as e:
                                logging.warning(f"Error reading existing catalogue: {e}")
                        
                        # Calculate new hash if not found in existing data
                        if not md5_hash:
                            with open(p, "rb") as f:
                                md5_hash = hashlib.md5(f.read()).hexdigest()
                    
                    # Add file to the list of all processed files
                    chunk_number = idx
                    all_files.append((p, chunk_number, md5_hash, size))
                    processed_paths.add(p)
                    chunk_file_list.append((p, size))
                except Exception as e:
                    # Handle errors during file processing
                    logging.error(f"Error processing {p}: {e}")
                    all_files.append((p, idx, "error", size))
                    processed_paths.add(p)
        
        # Mark this chunk as processed
        processed_chunks.add(idx)
        logging.info(f"Processed {len(chunk_file_list)} files for chunk {idx}")
        
        # Create symlinks for all files in this chunk
        make_symlinks(chunk_files, idx)
    
    # Create catalogue files (JSON, CSV, HTML) for each processed chunk
    for idx in processed_chunks:
        meta_dir = os.path.join(f"chunk_{idx}", "_META")
        create_json_catalogue(meta_dir, all_files)
        create_csv_catalogue(meta_dir, all_files)
        create_html_catalogue(meta_dir, all_files, idx)
    
    # Return the complete list of processed files
    return all_files


def calculate_chunks(files, burnt_chunks, chunk_size, existing_chunks=None):
    """
    Assign files to chunks while trying to keep files from the same directory together.
    Uses a best-fit bin packing algorithm to optimize chunk usage.
    
    Args:
        files: List of tuples (file_path, size) for files to be assigned
               Each tuple contains:
               - file_path: Absolute path to the file (string)
               - size: Size of the file in bytes (integer)
        burnt_chunks: List of chunk numbers that are marked as complete/burnt
        chunk_size: Maximum size in bytes for each chunk
        existing_chunks: Optional list of tuples (file_paths, total_size) representing existing chunks
                        Each tuple contains:
                        - file_paths: List of absolute file paths already in this chunk
                        - total_size: Current total size of this chunk in bytes
        
    Returns:
        tuple: (chunks, file_to_chunk) where chunks is a list of (file_paths, size) and
               file_to_chunk is a dict mapping each file path to its chunk number
    """
    # Group files by directory to keep related files together
    dir_to_files = {}
    for path, size in files:
        dir_path = os.path.dirname(path)
        if dir_path not in dir_to_files:
            dir_to_files[dir_path] = []
        dir_to_files[dir_path].append((path, size))
    
    # Calculate total size for each directory and sort by size (largest first)
    dirs_with_sizes = [(dir_path, sum(size for _, size in files_in_dir)) 
                       for dir_path, files_in_dir in dir_to_files.items()]
    dirs_with_sizes.sort(key=lambda x: x[1], reverse=True)
    
    # Initialize chunks with existing chunks if provided, otherwise start with empty list
    chunks = []
    if existing_chunks:
        # Make a copy of existing chunks
        for idx, (files, size) in enumerate(existing_chunks):
            chunks.append((files.copy(), size))
    
    # First pass: try to fit entire directories into chunks
    for dir_path, dir_size in dirs_with_sizes:
        # Skip directories too large for a single chunk
        if dir_size > chunk_size:
            continue
            
        # Find best fit chunk for this directory using best-fit algorithm
        best_fit_index = None
        best_fit_remaining = float('inf')
        
        for i, (chunk_files, current_size) in enumerate(chunks):
            # Skip burnt chunks - add 1 because chunk numbers are 1-indexed
            chunk_number = i + 1
            if chunk_number in burnt_chunks:
                continue
                
            # Check if directory fits in this chunk
            if current_size + dir_size <= chunk_size:
                remaining = chunk_size - (current_size + dir_size)
                if remaining < best_fit_remaining:
                    best_fit_index = i
                    best_fit_remaining = remaining
        
        files_in_dir = dir_to_files[dir_path]
        if best_fit_index is not None:
            # Add directory files to best fit chunk (keep as tuples)
            for file_tuple in files_in_dir:
                chunks[best_fit_index][0].append(file_tuple)  # Append the whole tuple
            chunks[best_fit_index] = (chunks[best_fit_index][0], chunks[best_fit_index][1] + dir_size)
        else:
            # Create a new chunk for this directory - maintain (path, size) tuples
            chunks.append((files_in_dir.copy(), dir_size))  # files_in_dir already contains tuples

    # Identify files from directories that were too large for a single chunk
    remaining_files = []
    #print(f"file exmaple : {files}")
    for path, size in files:
        dir_path = os.path.dirname(path)
        if dir_path in dir_to_files and sum(s for _, s in dir_to_files[dir_path]) > chunk_size:
            remaining_files.append((path, size))
    
    # Sort remaining files by size (largest first)
    remaining_files.sort(key=lambda x: x[1], reverse=True)
    
    # Second pass: assign individual files that couldn't be grouped by directory
    for path, size in remaining_files:
        best_fit_index = None
        best_fit_remaining = float('inf')
        
        # Find best fit chunk for this file
        for i, (chunk_files, current_size) in enumerate(chunks):
            chunk_number = i + 1
            if chunk_number in burnt_chunks:
                continue
                
            if current_size + size <= chunk_size:
                remaining = chunk_size - (current_size + size)
                if remaining < best_fit_remaining:
                    best_fit_index = i
                    best_fit_remaining = remaining
        
        if best_fit_index is not None:
            # Add file to best fit chunk (as a tuple)
            file_tuple = (path, size)
            chunks[best_fit_index][0].append(file_tuple)  # Append the whole tuple
            chunks[best_fit_index] = (chunks[best_fit_index][0], chunks[best_fit_index][1] + size)
        else:
            # Create a new chunk for this file (as a tuple)
            chunks.append(([file_tuple], size))  # Keep as a list of tuples
    
    # Create mapping of files to their assigned chunks - update to handle tuples
    file_to_chunk = {}
    for chunk_idx, (chunk_files, _) in enumerate(chunks, start=1):
        for file_tuple in chunk_files:
            file_path = file_tuple[0] if isinstance(file_tuple, tuple) else file_tuple
            file_to_chunk[file_path] = chunk_idx
    
    # Check for any chunks that exceed the maximum size
    for i, (files, size) in enumerate(chunks):
        if size > chunk_size:
            logging.warning(f"Chunk {i+1} exceeds maximum size: {size/(1024**3):.2f} GB > {chunk_size/(1024**3):.2f} GB")
            
    return chunks, file_to_chunk

def create_json_catalogue(meta_dir, all_files):
    """
    Create a JSON catalogue of all files in the metadata directory.
    
    Args:
        meta_dir: Directory where the catalogue should be created
        all_files: List of tuples (path, chunk_number, md5, size) for all processed files
    """
    json_file = os.path.join(meta_dir, "catalogue.json")
    with open(json_file, "w") as f:
        json.dump(
            [{"path": p, "chunk_number": chunk_number, "md5": md5, "size": size} for p, chunk_number, md5, size in all_files],
            f,
            indent=2
        )

def create_csv_catalogue(meta_dir, all_files):
    """
    Create a CSV catalogue of all files in the metadata directory.
    
    Args:
        meta_dir: Directory where the catalogue should be created
        all_files: List of tuples (path, chunk_number, md5, size) for all processed files
    """
    csv_file = os.path.join(meta_dir, "catalogue.csv")
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Path", "Chunk Number", "MD5 Hash", "Size (bytes)"])
        for p, chunk_number, md5, size in all_files:
            writer.writerow([p, chunk_number if chunk_number else "None", md5, size])

def create_html_catalogue(meta_dir, all_files, idx):
    """
    Create an HTML catalogue of all files in the metadata directory.
    The files belonging to the current chunk are highlighted.
    
    Args:
        meta_dir: Directory where the catalogue should be created
        all_files: List of tuples (path, chunk_number, md5, size) for all processed files
        idx: Current chunk number for highlighting files in this chunk
    """
    html_file = os.path.join(meta_dir, "catalogue.html")
    with open(html_file, "w") as f:
        f.write(generate_html_catalogue(all_files, idx))

def generate_html_catalogue(all_files, idx):
    """
    Generate HTML content for the catalogue.
    
    Args:
        all_files: List of tuples (path, chunk_number, md5, size) for all processed files
        idx: Current chunk number for highlighting files in this chunk
        
    Returns:
        str: HTML content for the catalogue
    """
    rows = "\n".join(
        f"""        <tr{' class="in-chunk"' if chunk_number == idx else ''}>
            <td>{p}</td>
            <td>{chunk_number if chunk_number else 'None'}</td>
            <td>{md5}</td>
            <td>{size}</td>
        </tr>""" for p, chunk_number, md5, size in sorted(all_files, key=lambda x: x[0])
    )
    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Chunk {idx} Catalogue</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ font-family: Arial, sans-serif; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}
        tr.in-chunk {{ background-color: #ffffcc; }}
    </style>
</head>
<body>
    <h1>Chunk {idx} Catalogue</h1>
    <table>
        <tr>
            <th>Path</th>
            <th>Chunk Number</th>
            <th>MD5 Hash</th>
            <th>Size (bytes)</th>
        </tr>
{rows}
    </table>
</body>
</html>
"""

def traverse_with_listdir(top_dir, ignore_pattern=None):
    """
    Recursively traverse directories to find files using os.listdir.
    
    This function provides better progress reporting than os.walk by showing
    a running count of files and directories processed. It also handles
    duplicate files by checking real paths.
    
    Args:
        top_dir: Root directory to start traversal from
        ignore_pattern: Optional regex pattern for files/directories to ignore
        
    Returns:
        List of tuples (file_path, size) for all files found
    """
    files = []
    existing_files = set()
    seen_realpaths = set()
    catalogue_path = os.path.join(os.getcwd(), "catalogue.json")
    if os.path.exists(catalogue_path):
        try:
            with open(catalogue_path, "r") as f:
                existing_files = {os.path.basename(entry["path"]) for entry in json.load(f)}
        except Exception as e:
            logging.error(f"Error loading existing catalogue: {e}")
    try:
        items = os.listdir(top_dir)
        # Track global counters for total items processed
        global total_files_processed, total_dirs_processed
        if 'total_files_processed' not in globals():
            total_files_processed = 0
        if 'total_dirs_processed' not in globals():
            total_dirs_processed = 0
        total_dirs_processed += 1
        sys.stdout.write(f"\r({total_files_processed}/{total_dirs_processed}) (files / directories) so far...")
        sys.stdout.flush()
        for item in items:
            path = os.path.join(top_dir, item)
            if ignore_pattern and ignore_pattern.search(item):
                continue
            if os.path.isdir(path):
                files.extend(traverse_with_listdir(path, ignore_pattern))
            elif os.path.isfile(path):
                
                if item in existing_files:
                    continue
                real_path = os.path.realpath(path)
                if real_path in seen_realpaths:
                    continue
                seen_realpaths.add(real_path)
                size = os.path.getsize(real_path)
                files.append((real_path, size))
                total_files_processed += 1
    except (PermissionError, FileNotFoundError) as e:
        logging.error(f"Error accessing {top_dir}: {e}")
    return files

def walker(top_dir):
    """
    Alternative directory traversal function using os.walk.
    
    Uses os.walk to recursively find all files in a directory tree.
    Respects global ignore_regex pattern for filtering files and directories.
    
    Args:
        top_dir: Root directory to start traversal from
        
    Returns:
        List of tuples (file_path, size) for all files found
    """
    files = []
    for root, dirs, filenames in os.walk(top_dir):
        if root == top_dir:
            dirs[:] = [d for d in dirs if not (ignore_regex and ignore_regex.search(d))]
        for f in filenames:
            if ignore_regex and ignore_regex.search(f):
                continue
            path = os.path.join(root, f)
            size = os.path.getsize(path)
            files.append((path, size))
    return files

def make_symlinks(paths, idx):
    dname = f"chunk_{idx}"
    os.makedirs(dname, exist_ok=True)
    logging.debug(f"Creating symlinks in directory: {dname}")
    
    # Get the base directory from args or determine it from known files
    base_dir = args.directory if hasattr(args, 'directory') else get_base_directory()
    logging.info(f"Base directory for symlinks: {base_dir}")
    
    for p_item in paths:
        # Handle both tuple format and plain path format
        p = p_item[0] if isinstance(p_item, tuple) else p_item
        
        # Always use basename if path is outside base_dir to avoid "../" in paths
        if not base_dir or not p.startswith(base_dir):
            rel_path = os.path.basename(p)
        else:
            # Safe to create relative path since file is under base_dir
            rel_path = os.path.relpath(p, base_dir)
        
        # Ensure rel_path doesn't escape the chunk directory
        if rel_path.startswith(".."):
            # Replace parent directory references with a safe alternative
            rel_path = rel_path.replace("..", "parent")
            # Or just use the basename:
            # rel_path = os.path.basename(p)
        
        link_name = os.path.join(dname, rel_path)
        
        # Create parent directories
        #print(f"Creating directory for {p} -> {link_name}")
        os.makedirs(os.path.dirname(link_name), exist_ok=True)
        
        # Create symlink if it doesn't exist
        if not os.path.exists(link_name):
            try:
                os.symlink(p, link_name)
                logging.debug(f"Created symlink: {link_name} -> {p}")
            except FileExistsError:
                logging.debug(f"Symlink already exists: {link_name}")
            except Exception as e:
                logging.error(f"Failed to create symlink {link_name}: {e}")
        else:
            logging.debug(f"Path already exists: {link_name}")

def get_base_directory():
    """
    Try to determine the base directory from known files in chunks_meta.json.
    
    This function is used when the base directory isn't explicitly provided
    via command line arguments. It finds the common parent directory of all
    files in the archive to create consistent relative paths for symlinks.
    
    Returns:
        str: Common base directory path or empty string if not determinable
    """
    preexisting = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(preexisting):
        return ''
        
    try:
        with open(preexisting, "r") as f:
            chunks_meta = json.load(f)
        known_files = chunks_meta.get("known_files", [])
        
        if known_files:
            # Find the common prefix among all file paths
            paths = [f[0] for f in known_files]
            common_prefix = os.path.commonpath(paths) if paths else ''
            logging.debug(f"Determined base directory: {common_prefix}")
            return common_prefix
    except Exception as e:
        logging.error(f"Error determining base directory: {e}")
    
    return ''

def restore_from_metadata(args):
    """
    Restore chunk directories and symlinks from a backup metadata file.
    
    This command:
    1. Loads chunk structure from the specified metadata file
    2. Recreates chunk directories based on the metadata
    3. Creates symlinks for files that still exist
    4. Creates an updated metadata file reflecting current state
    
    Args:
        args: Command line arguments with metafile path and optional 'yes' flag
    """
    meta_path = args.metafile
    logging.info(f"Restoring from metadata file: {meta_path}")
    
    # Check if the metadata file exists
    if not os.path.exists(meta_path):
        logging.error(f"Metadata file not found: {meta_path}")
        sys.exit(1)
    
    # Load the metadata
    try:
        with open(meta_path, "r") as f:
            chunks_meta = json.load(f)
    except Exception as e:
        logging.error(f"Error loading metadata file: {e}")
        sys.exit(1)
    
    # Extract metadata components
    known_files = chunks_meta.get("known_files", [])
    burnt_chunks = chunks_meta.get("burnt_chunks", [])
    chunk_size_setting = chunks_meta.get("chunk_size_setting", 20)  # Default 20GB
    
    if not known_files:
        logging.error("No files found in metadata")
        sys.exit(1)
    
    # Confirm before proceeding unless --yes flag is used
    if not hasattr(args, 'yes') or not args.yes:
        confirm = input(f"This will recreate chunk directories based on {meta_path}. Continue? (y/N): ")
        if confirm.lower() != 'y':
            logging.info("Restore operation cancelled.")
            return
    
    # Group files by chunk
    files_by_chunk = {}
    for file_data in known_files:
        path, chunk_number, md5, size = file_data
        if chunk_number not in files_by_chunk:
            files_by_chunk[chunk_number] = []
        files_by_chunk[chunk_number].append((path, size, md5))
    
    # Track missing and successfully restored files
    missing_files = []
    restored_files = []

    # get a list of directories potentially containing the original files based on common path part in the metadata
    base_dir = get_base_directory_from_metadata(known_files)
    
    if base_dir:
        fs_content = os.walk(base_dir)
        
    else:
        logging.warning("No common base directory found in metadata. Using current working directory.")
    
    # Process each chunk
    total_chunks = len(files_by_chunk)
    for chunk_idx, (chunk_number, files) in enumerate(sorted(files_by_chunk.items()), 1):
        sys.stdout.write(f"\rRestoring chunks: {chunk_idx}/{total_chunks} ({(chunk_idx/total_chunks)*100:.1f}%)")
        sys.stdout.flush()
        
        # Create chunk directory
        chunk_dir = f"chunk_{chunk_number}"
        os.makedirs(chunk_dir, exist_ok=True)
        
        # Create metadata directory
        meta_dir = os.path.join(chunk_dir, "_META")
        os.makedirs(meta_dir, exist_ok=True)
        
        # Process files in this chunk
        chunk_files = []
        for file_path, size, md5 in files:
            # Check if file still exists
            if os.path.exists(file_path):
                # File exists, add to restored files
                current_size = os.path.getsize(file_path)
                restored_files.append((file_path, chunk_number, md5, current_size))
                chunk_files.append((file_path, current_size))
                
                # Create relative path for symlink
                base_dir = get_base_directory_from_metadata(known_files)
                
                # Create symlink path safely (avoiding .. paths)
                if not base_dir or not file_path.startswith(base_dir):
                    rel_path = os.path.basename(file_path)
                else:
                    rel_path = os.path.relpath(file_path, base_dir)
                
                # Ensure rel_path doesn't escape the chunk directory
                if rel_path.startswith(".."):
                    rel_path = rel_path.replace("..", "parent")
                
                link_name = os.path.join(chunk_dir, rel_path)
                
                # Create parent directories for symlink
                os.makedirs(os.path.dirname(link_name), exist_ok=True)
                
                # Create symlink if it doesn't exist
                if not os.path.exists(link_name):
                    try:
                        os.symlink(file_path, link_name)
                        logging.debug(f"Created symlink: {link_name} -> {file_path}")
                    except Exception as e:
                        logging.error(f"Failed to create symlink {link_name}: {e}")
            else:
                # File doesn't exist anymore, add to missing files
                missing_files.append((file_path, chunk_number, md5, size))
    
    # Finish progress display with newline
    sys.stdout.write("\n")
    sys.stdout.flush()
    
    # Create catalogues for each chunk
    logging.info("Creating catalogues for all chunks...")
    all_files = restored_files + missing_files  # Combine restored and missing files
    
    for chunk_number in files_by_chunk.keys():
        meta_dir = os.path.join(f"chunk_{chunk_number}", "_META")
        create_json_catalogue(meta_dir, all_files)
        create_csv_catalogue(meta_dir, all_files)
        create_html_catalogue(meta_dir, all_files, chunk_number)
    
    # Calculate sizes for each chunk
    chunk_sizes = {}
    for path, chunk_number, _, size in restored_files:
        if chunk_number not in chunk_sizes:
            chunk_sizes[chunk_number] = 0
        chunk_sizes[chunk_number] += size
    
    # Save updated chunks_meta.json
    updated_meta = {
        "known_files": all_files,
        "burnt_chunks": burnt_chunks,
        "chunk_sizes": {str(chunk): f"{size/(1024**2):.1f} MB" for chunk, size in chunk_sizes.items()},
        "chunk_size_setting": chunk_size_setting
    }
    
    with open("chunks_meta.json", "w") as f:
        json.dump(updated_meta, f, indent=2)
    
    # Report results
    logging.info(f"Restore complete: {len(restored_files)} files restored, {len(missing_files)} files missing")
    
    if missing_files:
        logging.warning("Some files from the original metadata no longer exist:")
        for i, (path, chunk, _, _) in enumerate(missing_files[:10], 1):
            logging.warning(f"  {i}. Missing: {path} (was in chunk {chunk})")
        
        if len(missing_files) > 10:
            logging.warning(f"  ... and {len(missing_files) - 10} more")

def get_base_directory_from_metadata(known_files):
    """
    Determine the base directory from a list of files in metadata.
    
    Args:
        known_files: List of (path, chunk, md5, size) tuples from metadata
        
    Returns:
        str: Common base path for files or empty string if not determinable
    """
    if not known_files:
        return ''
    
    try:
        # Find common prefix among existing files
        paths = [f[0] for f in known_files if os.path.exists(f[0])]
        if not paths:
            return ''
        
        common_prefix = os.path.commonpath(paths)
        logging.debug(f"Determined base directory from metadata: {common_prefix}")
        return common_prefix
    except Exception as e:
        logging.error(f"Error determining base directory from metadata: {e}")
        return ''

def list_tracked_directories(args):
    """
    List all directories that have been added to chunks.
    
    This command displays information about all directories that were
    processed using the 'add' command, including when they were added,
    how many files they contained, and their total size.
    
    Args:
        args: Command line arguments with optional json flag
    """
    logging.info("Listing tracked directories...")
    
    # Check if chunks_meta.json exists
    meta_file = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(meta_file):
        logging.error("No chunks_meta.json found. No tracked directories.")
        return
    
    # Load the metadata
    try:
        with open(meta_file, "r") as f:
            chunks_meta = json.load(f)
    except Exception as e:
        logging.error(f"Error loading metadata file: {e}")
        return
    
    # Get tracked directories
    tracked_directories = chunks_meta.get("tracked_directories", [])
    
    if not tracked_directories:
        logging.info("No directories have been tracked yet.")
        return
    
    # Output in JSON format if requested
    if hasattr(args, 'json') and args.json:
        print(json.dumps(tracked_directories, indent=2))
        return
    
    # Output in human-readable format
    print("\nTracked directories:")
    print("=" * 80)
    print(f"{'Directory':<40} {'Added on':<25} {'Files':<8} {'Size':<15}")
    print("-" * 80)
    
    for i, dir_info in enumerate(tracked_directories, 1):
        # Format timestamp for display
        ts = dir_info.get('timestamp', 'Unknown')
        try:
            # Try to parse ISO format timestamp to make it more readable
            import datetime
            
            dt = datetime.datetime.fromisoformat(ts)
            ts_display = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            ts_display = ts
        
        # Format directory path (truncate if too long)
        dir_path = dir_info.get('directory', 'Unknown')
        if len(dir_path) > 38:
            dir_path = "..." + dir_path[-35:]
            
        # Format size in human-readable form
        size_bytes = dir_info.get('total_size_bytes', 0)
        if size_bytes > 1024**3:
            size_str = f"{size_bytes/(1024**3):.2f} GB"
        elif size_bytes > 1024**2:
            size_str = f"{size_bytes/(1024**2):.2f} MB"
        elif size_bytes > 1024:
            size_str = f"{size_bytes/1024:.2f} KB"
        else:
            size_str = f"{size_bytes} bytes"
            
        # Format ignore pattern if present
        ignore = dir_info.get('ignore_pattern', '')
        ignore_str = f" (ignore: '{ignore}')" if ignore else ""
        
        # Print directory information
        print(f"{dir_path:<40} {ts_display:<25} {dir_info.get('files_count', 0):<8} {size_str:<15}{ignore_str}")
    
    print("=" * 80)
    print(f"Total: {len(tracked_directories)} directories")

def rescan_directories(args):
    """
    Rescan all previously added directories for new files.
    
    This command:
    1. Retrieves the list of tracked directories from metadata
    2. Processes each directory as if it were newly added
    3. Only adds files that weren't previously included
    4. Creates symlinks and updates catalogues
    
    Args:
        args: Command line arguments with optional 'md5' and 'yes' flags
    """
    logging.info("Rescanning previously added directories for new files...")
    
    # Check if chunks_meta.json exists
    meta_file = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(meta_file):
        logging.error("No chunks_meta.json found. Nothing to rescan.")
        return
    
    # Load existing metadata
    try:
        with open(meta_file, "r") as f:
            chunks_meta = json.load(f)
    except Exception as e:
        logging.error(f"Error loading metadata file: {e}")
        return
        
    # Get tracked directories
    tracked_directories = chunks_meta.get("tracked_directories", [])
    directory_ignore_patterns = chunks_meta.get("directory_ignore_patterns", {})
    
    if not tracked_directories:
        logging.warning("No tracked directories found in metadata. Nothing to rescan.")
        return
    
    # Confirm before proceeding unless --yes flag is used
    if not hasattr(args, 'yes') or not args.yes:
        num_dirs = len(tracked_directories)
        confirm = input(f"This will rescan {num_dirs} previously added director{'y' if num_dirs == 1 else 'ies'}. Continue? (y/N): ")
        if confirm.lower() != 'y':
            logging.info("Rescan operation cancelled.")
            return
    
    # Get settings and other metadata
    burnt_chunks = chunks_meta.get("burnt_chunks", [])
    known_files = chunks_meta.get("known_files", [])
    chunk_size_setting = chunks_meta.get("chunk_size_setting", 20)  # Default 20GB
    
    # Set chunk size for calculation
    global CHUNK_SIZE
    global ignore_regex

    CHUNK_SIZE = chunk_size_setting * 1024**3
    
    # Initialize list for all files found during rescanning
    all_files = []
    
    # Track statistics
    total_new_files = 0
    total_dirs_rescanned = 0
    dirs_with_new_files = 0
    
    # Create a set of known file paths for quick lookup
    known_file_paths = {f[0] for f in known_files}
    
    # Process each tracked directory
    for dir_idx, dir_info in enumerate(tracked_directories, 1):
        directory = dir_info.get('directory', '')
        
        # Get ignore pattern for this directory
        # First try from directory_ignore_patterns (new approach)
        # Then fall back to ignore_pattern in dir_info (old approach)
        ignore_pattern = directory_ignore_patterns.get(directory, dir_info.get('ignore_pattern', ''))
        
        
        if not directory or not os.path.exists(directory) or not os.path.isdir(directory):
            logging.warning(f"Directory not found or not accessible: {directory}")
            continue
        
        # Set up ignore regex for this directory
        ignore_regex = re.compile(ignore_pattern) if ignore_pattern else None
        
        # Log progress with ignore pattern information
        logging.info(f"Rescanning directory {dir_idx}/{len(tracked_directories)}: {directory} (ignore pattern: '{ignore_pattern}')")
        
        # Scan the directory
        files = traverse_with_listdir(directory, ignore_regex)
        
        # Find new files that weren't in the previous scan
        new_files = [f for f in files if f[0] not in known_file_paths]
        
        # Update statistics
        if new_files:
            dirs_with_new_files += 1
            total_new_files += len(new_files)
            logging.info(f"Found {len(new_files)} new files in {directory}")
        else:
            logging.info(f"No new files found in {directory}")
        
        # Add all found files to our list
        all_files.extend(new_files)
        total_dirs_rescanned += 1
        
        # Update directory info with new counts and sizes
        dir_total_size = sum(size for _, size in files)
        dir_info.update({
            "files_count": len(files),
            "total_size_bytes": dir_total_size,
            "total_size_gb": dir_total_size / (1024**3),
            "ignore_pattern": ignore_pattern,  # Ensure ignore pattern is saved in directory info
            "last_rescanned": datetime.datetime.now().isoformat()
        })
        
        # Also update directory_ignore_patterns
        directory_ignore_patterns[directory] = ignore_pattern
    
    # If no new files found, report and exit
    if total_new_files == 0:
        logging.info(f"Rescanned {total_dirs_rescanned} directories. No new files found.")
        
        # Save updated directory metadata even if no new files were found
        chunks_meta["tracked_directories"] = tracked_directories
        chunks_meta["directory_ignore_patterns"] = directory_ignore_patterns
        with open("chunks_meta.json", "w") as f:
            json.dump(chunks_meta, f, indent=2)
        
        return
    
    # Report found files
    logging.info(f"Rescanned {total_dirs_rescanned} directories, found {total_new_files} new files in {dirs_with_new_files} directories.")
    
    # Reconstruct existing chunks from metadata
    chunk_sizes = chunks_meta.get("chunk_sizes", {})
    existing_chunks = []
    for chunk_id, size_str in chunk_sizes.items():
        chunk_id = int(chunk_id)
        # Get files that belong to this chunk
        files_in_chunk = []
        for file_path in [f[0] for f in known_files if f[1] == chunk_id]:
            # Find size from known_files
            size = next((f[3] for f in known_files if f[0] == file_path), 0)
            files_in_chunk.append((file_path, size))
        
        # Convert size string back to bytes
        size_mb = float(size_str.split()[0])
        size_bytes = int(size_mb * 1024**2)
        existing_chunks.append((files_in_chunk, size_bytes))
    
    logging.info(f"Adding new files to existing chunks...")
    
    # Find new files that weren't processed before
    new_files = [f for f in all_files if f[0] not in known_file_paths]
    print(new_files)
    
    # Calculate chunks for new files
    if new_files:
        if existing_chunks:
            # Add new files to existing chunks where possible
            chunks, file_to_chunk = calculate_chunks(new_files, burnt_chunks, CHUNK_SIZE, existing_chunks)
        else:
            # Create new chunks for all files
            chunks, file_to_chunk = calculate_chunks(new_files, burnt_chunks, CHUNK_SIZE)
    else:
        # This shouldn't happen but handle it just in case
        logging.warning("No new files found after scanning. This is unexpected.")
        return
    
    # Process chunks to create symlinks and catalogues
    args.md5 = hasattr(args, 'md5') and args.md5  # Ensure md5 flag is set correctly
    all_new_files = process_chunks(chunks, all_files, file_to_chunk, known_files)
    
    # Calculate sum of sizes for each chunk
    chunk_sizes = {}
    for path, chunk_number, _, size in all_new_files:
        if chunk_number is not None:
            if chunk_number not in chunk_sizes:
                chunk_sizes[chunk_number] = 0
            chunk_sizes[chunk_number] += size
    
    # Save updated chunks_meta.json
    updated_meta = {
        "known_files": all_new_files,
        "burnt_chunks": burnt_chunks,
        "chunk_sizes": {str(chunk): f"{size/(1024**2):.1f} MB" for chunk, size in chunk_sizes.items()},
        "chunk_size_setting": chunk_size_setting,
        "tracked_directories": tracked_directories,
        "directory_ignore_patterns": directory_ignore_patterns
    }
    
    with open("chunks_meta.json", "w") as f:
        json.dump(updated_meta, f, indent=2)
    
    # Log chunk sizes for verification
    for chunk, size in sorted(chunk_sizes.items()):
        logging.info(f"Chunk {chunk}: {size/(1024**3):.2f} GB")
    
    logging.info(f"Rescan complete. Added {total_new_files} new files to {len(chunks)} chunks.")

def output_iso_command(args):
    """
    Output a genisoimage syntax suggestion to make an ISO out of the next unburnt chunk 
    """

    meta_file = os.path.join(os.getcwd(), "chunks_meta.json")
    if not os.path.exists(meta_file):
        logging.error("No chunks_meta.json found. Cannot output ISO command.")
        return
    
    try:
        with open(meta_file, "r") as f:
            chunks_meta = json.load(f)
    except Exception as e:
        logging.error(f"Error loading metadata file: {e}")
        return
    burnt_chunks = chunks_meta.get("burnt_chunks", [])
    existing_chunks = chunks_meta.get("chunk_sizes", {})
    
    #check if chunk_no has been specified
    if hasattr(args, 'chunk_no') and args.chunk_no >0 :
        next_chunk = args.chunk_no
    else:
        # Find the next unburnt chunk
        next_chunk = None
        for chunk_number in range(1, len(existing_chunks) + 1):
            if chunk_number not in burnt_chunks:
                next_chunk = chunk_number
                break
    
    if next_chunk is None:
        logging.info("All chunks have been burnt. No more ISO commands to output.")
        return

    #check if the size of the suggested chunk is at least 80% of the chunk size setting
    chunk_size_setting = chunks_meta.get("chunk_size_setting", 20) * 1024**3  # Default 20GB
    chunk_size = float(existing_chunks.get(str(next_chunk), 0).split(' ')[0])*1024**2  # Convert MB to ~bytes
    
    if chunk_size < 0.8 * chunk_size_setting:
        logging.info(f"Chunk {next_chunk} is too small ({chunk_size/(1024**3):.2f} GB). Consider adding more files before burning.")
    
    #finally, output the command to make ISO image 
    print(f"genisoimage -o chunk_{next_chunk}.iso -f -R -J -V 'Chunk {next_chunk}' chunk_{next_chunk}/")
    

    



if __name__ == "__main__":
    main()

