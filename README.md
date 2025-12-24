# GB2PR Project

AWS S3 and EMR data processing pipeline for feed management and file operations.

## Overview

This project processes data feeds from AWS S3, handles file operations, and manages feed processing workflows using EMR.

## Project Structure

```
GB2PR_PROJECT/
├── cmd/
│   └── main/              # Application entry point
│       └── godcrazycode.py
├── pkg/
│   ├── common/            # Common utilities
│   │   ├── CommonUtlity.py
│   │   ├── compareFileNameSize.py
│   │   └── recordCount.py
│   └── config/            # Configuration files
│       └── configs.ini
├── internal/
│   ├── processors/        # Feed processing logic
│   │   ├── feedProcessing.py
│   │   ├── latestfeedprocessing.py
│   │   └── merging.py
│   └── monitors/          # Monitoring and logging
│       ├── logMonitor.py
│       └── WatchDog.py
├── tests/                 # Test packages
├── logs/                  # Application logs
├── BUILD                  # Bazel root build file
├── MODULE.bazel           # Bazel module configuration
├── .bazelrc               # Bazel configuration
└── requirements.txt       # Python dependencies
```

## Features

- **Feed Processing**: Process and manage data feeds from S3
- **File Operations**: Handle file extraction, comparison, and merging
- **Monitoring**: Logging and watchdog functionality
- **Configuration**: Centralized configuration management

## Prerequisites

- Python 3.9+
- Bazel 7.0+
- AWS credentials configured
- Access to S3 buckets and EMR clusters

## Installation

### Using Bazel (Recommended)

```bash
# Build the project
bazel build //...

# Build specific package
bazel build //pkg/common:common

# Run the main application
bazel run //cmd/main:main -- <target-date>
```

### Using Python directly

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python3 cmd/main/godcrazycode.py <target-date>
```

## Usage

### Running the Application

```bash
# Using Bazel
bazel run //cmd/main:main -- 2021-11-02

# Using Python
python3 cmd/main/godcrazycode.py 2021-11-02
```

### Configuration

Edit `pkg/config/configs.ini` to configure:
- AWS credentials
- S3 bucket paths
- EMR cluster settings
- Processing parameters

## Development

### Building

```bash
# Build everything
bazel build //...

# Build specific package
bazel build //pkg/common:common
bazel build //internal/processors:processors
```

### Testing

```bash
# Run all tests
bazel test //...

# Run specific test
bazel test //tests:test_package_import
```

### Code Organization

- **pkg/**: Public packages that can be imported by other projects
- **internal/**: Internal implementation details (not for external use)
- **cmd/**: Application entry points
- **tests/**: Test packages

## Dependencies

See `requirements.txt` for Python dependencies.

## Logging

Logs are written to the `logs/` directory with format: `YYYYMMDD_HH.log`

## License

[Add your license here]

## Contributing

[Add contributing guidelines here]

