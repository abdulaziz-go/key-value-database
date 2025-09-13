package main

import (
	"fmt"
	"strings"

	"key-value-storage/bitcask"
)

func main() {
	bc, err := bitcask.Open("")
	if err != nil {
		fmt.Printf("BitCask ochishda xatolik: %v\n", err)
		return
	}
	defer bc.Close()

	for {
		fmt.Print("Buyruq kiriting put/get/delete/exit: ")
		var cmdInput string
		_, err := fmt.Scanln(&cmdInput)
		if err != nil {
			continue
		}
		cmd := strings.TrimSpace(strings.ToLower(cmdInput))

		if cmd == "exit" {
			break
		}

		switch cmd {
		case "put":
			fmt.Print("Kalitni kiriting: ")
			var keyInput string
			_, err := fmt.Scanln(&keyInput)
			if err != nil {
				fmt.Println("Xatolik: kalitni o'qib bo'lmadi")
				continue
			}
			key := strings.TrimSpace(keyInput)

			fmt.Print("Qiymatni kiriting: ")
			var valueInput string
			_, err = fmt.Scanln(&valueInput)
			if err != nil {
				fmt.Println("Xatolik: qiymatni o'qib bo'lmadi")
				continue
			}
			value := strings.TrimSpace(valueInput)

			if err := bc.Put(key, []byte(value)); err != nil {
				fmt.Printf("Xatolik: %v\n", err)
			} else {
				fmt.Println("Qiymat qo'shildi")
			}

		case "get":
			fmt.Print("Kalitni kiriting: ")
			var keyInput string
			_, err := fmt.Scanln(&keyInput)
			if err != nil {
				fmt.Println("Xatolik: kalitni o'qib bo'lmadi")
				continue
			}
			key := strings.TrimSpace(keyInput)

			val, err := bc.Get(key)
			if err != nil {
				fmt.Println("Kalit topilmadi")
			} else {
				fmt.Printf("Qiymat: %s\n", string(val))
			}

		case "delete":
			fmt.Print("Kalitni kiriting: ")
			var keyInput string
			_, err := fmt.Scanln(&keyInput)
			if err != nil {
				fmt.Println("Xatolik: kalitni o'qib bo'lmadi")
				continue
			}
			key := strings.TrimSpace(keyInput)

			if err := bc.Delete(key); err != nil {
				fmt.Println("Kalit o'chirilmadi yoki topilmadi")
			} else {
				fmt.Println("Kalit o'chirildi")
			}

		default:
			fmt.Println("Noma'lum buyruq, iltimos put/get/delete/exit tanlang")
		}
	}
}
