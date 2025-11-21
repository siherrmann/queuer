package helper

import "fmt"

func PrintInfo(format string, a ...any) {
	fmt.Printf("💡  "+format+"\n", a...)
}

func PrintSuccess(format string, a ...any) {
	fmt.Printf("✅  "+format+"\n", a...)
}

func PrintError(format string, a ...any) {
	fmt.Printf("❌  "+format+"\n", a...)
}
