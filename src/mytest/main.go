package mytest

import (
	"fmt"
	"os"
	"plugin"
)

func main() {
	// 打开插件
	p, err := plugin.Open("plugin.so")
	if err != nil {
		panic(err)
	}
	// 查找导出的变量或方法名
	sb, err := p.Lookup("Say")
	if err != nil {
		panic(err)
	}
	// 判断导出内容的类型
	sbs := sb.(func(string) string)

	if len(os.Args) > 2 {
		// 插件导出的变量数据
		str, err := p.Lookup("Addr")
		if err != nil {
			panic(err)
		}
		*str.(*string) = os.Args[2]
	}
	// 运行导出的方法
	fmt.Println(sbs(os.Args[1]))
}