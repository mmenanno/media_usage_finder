# Contributing to Media Usage Finder

Thank you for your interest in contributing to Media Usage Finder!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/media-usage-finder`
3. Create a branch: `git checkout -b feature/my-new-feature`
4. Make your changes
5. Run tests: `make test`
6. Commit your changes: `git commit -am 'Add new feature'`
7. Push to your fork: `git push origin feature/my-new-feature`
8. Create a Pull Request

## Development Setup

### Prerequisites

- Go 1.21 or higher
- Node.js (for Tailwind CSS)
- SQLite
- Make

### Building

```bash
# Install dependencies
make install-deps

# Build Tailwind CSS
make tailwind

# Build binary
make build

# Run locally
./bin/media-finder serve
```

## Code Style

- Follow Go conventions and best practices
- Use `gofmt` to format your code
- Add comments for exported functions and types
- Write tests for new functionality

## Pull Request Guidelines

- Keep pull requests focused on a single feature or fix
- Include tests for new functionality
- Update documentation as needed
- Ensure the build passes
- Follow the existing code style

## Testing

```bash
# Run all tests
make test

# Run with coverage
go test -cover ./...
```

## Reporting Issues

When reporting issues, please include:

- Go version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

## Questions?

Feel free to open an issue for questions or discussions.
