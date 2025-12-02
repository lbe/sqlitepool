# <Your Module Name>

[![Go Report Card](https://goreportcard.com/badge/github.com/lbe/sqlitepool)](https://goreportcard.com/report/github.com/lbe/sqlitepool)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/lbe/sqlitepool)](https://pkg.go.dev/github.com/lbe/sqlitepool)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Actions Status](https://github.com/lbe/sqlitepool/workflows/ci/badge.svg)](https://github.com/lbe/sqlitepool/actions)
[![codecov](https://codecov.io/gh/lbe/sqlitepool/branch/main/graph/badge.svg)](https://codecov.io/gh/lbe/sqlitepool)
[![Release](https://img.shields.io/github/release/lbe/sqlitepool.svg?style=flat-square)](RELEASE-NOTES.md)

This effort is just getting underway.  There is nothing working here at this time.

The goal of this project is to deliver a package that will generate a pool 
of connections that will behave similar to the the stdlib database/sql pool
but overcome some of its shortcomings for sqlite.  The primary shortcoming
from my experience is the inability to retain prepared queries when a 
connection is returned to the pool and to subsequently access these prepared
statements when a connection is received from the pool. 



## Installation

To use this module in your Go project, you can use the `go get` command:

```bash
$ go get -u github.com/lbe/sqlitepool
```

## Usage

TBD: Explain how users can import and use your module in their Go projects. Provide code examples if necessary.

```go
package main

import (
	"fmt"
	"github.com/lbe/sqlitepool"
)

func main() {
    // Example usage
    fmt.Println(your_module_name.YourFunction())
}
```

## Configuration

TBD: If your module has configuration options, document them here.

## Contributing

TBD: Explain how others can contribute to your project. Include guidelines for reporting issues, submitting feature requests, and making pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

TBD: Give credit to any contributors, libraries, or tools that your module depends on.

## Contact

Please contact me by adding issues to this repository.

## Release History

See the [RELEASE-NOTES.tpl.md](RELEASE-NOTES.tpl.md) file for details.

## Support

If you encounter any issues or have questions, feel free to [create an issue](https://github.com/lbe/sqlitepool/issues).
