package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

var randomReceiverName string = "asbcfjhawkljxqwejhcqihjvgkoelpwdvhgwcpq"
var globalServerIP string = "3.7.96.64"
var globalServerURLHTTP string = "http://" + globalServerIP + ":8080"
var globalServerURLWS string = "ws://" + globalServerIP + ":8080"
var globalPrompt string = ">"
var globalUserName string = ""
var globalPassword string = ""
var globalWsConnection *websocket.Conn = nil
var mutex = &sync.Mutex{}

func updateGlobalUserNameAndPassword(newUserName string, newPassword string) {
	mutex.Lock()
	globalUserName = newUserName
	globalPassword = newPassword
	mutex.Unlock()
}

func getGlobalUsername() string {
	mutex.Lock()
	defer mutex.Unlock()
	value := globalUserName
	return value
}

func getGlobalPassword() string {
	mutex.Lock()
	defer mutex.Unlock()
	value := globalPassword
	return value
}

func getGlobalWsConnection() *websocket.Conn {
	mutex.Lock()
	defer mutex.Unlock()
	value := globalWsConnection
	return value
}

func updateGlobalWsConnection(newConnection *websocket.Conn) {
	mutex.Lock()
	globalWsConnection = newConnection
	defer mutex.Unlock()
}

func signup(userName string, password string) (msg string, isSuccessful bool) {
	var mp = make(map[string]string)
	mp["userName"] = userName
	mp["password"] = password

	b := new(bytes.Buffer)
	jsonErr := json.NewEncoder(b).Encode(mp)
	if jsonErr != nil {
		return "Application error occured during json encoding!", false
	}
	resp, err := http.Post(globalServerURLHTTP+"/signup", "application/text; charset=utf-8", b)
	if err != nil {
		return "Chat server is either offline or not accepting new connections! Please try later! Error msg: " + err.Error(), false
	}
	byteBodyArr, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "Error occured while reading response body!, Error msg: " + err.Error(), false
	}
	var decodedData string
	jsonErr = json.Unmarshal(byteBodyArr, &decodedData)
	if jsonErr != nil {
		return "Application error occured during json encoding!", false
	}
	if resp.StatusCode == http.StatusOK {
		return decodedData, true
	}
	return decodedData, false
}

func signin(userName string, password string) (msg string, isSuccessful bool) {
	var mp = make(map[string]string)
	mp["userName"] = userName
	mp["password"] = password

	b := new(bytes.Buffer)
	jsonErr := json.NewEncoder(b).Encode(mp)
	if jsonErr != nil {
		return "Application error occured during json encoding!", false
	}
	resp, err := http.Post(globalServerURLHTTP+"/signin", "application/text; charset=utf-8", b)
	if err != nil {
		return "Chat server is either offline or not accepting new connections! Please try later! Error msg: " + err.Error(), false
	}
	byteBodyArr, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "Error occured while reading response body!, Error msg: " + err.Error(), false
	}

	var decodedData string
	jsonErr = json.Unmarshal(byteBodyArr, &decodedData)
	if jsonErr != nil {
		return "Application error occured during json encoding!", false
	}
	if resp.StatusCode == http.StatusOK {
		return decodedData, true
	}
	return decodedData, false
}

func receiveMessages() {
	for {
		_, message, err := getGlobalWsConnection().ReadMessage()
		if err != nil {
			fmt.Println("Error occured while reading messages:", err)
			fmt.Println("This error could be caused by the chat server going offline!")
			updateGlobalUserNameAndPassword("", "")
			updateGlobalWsConnection(nil)
			fmt.Print(getGlobalUsername() + " " + globalPrompt + " ")
			return
		}
		fmt.Printf("Received message: %s\n"+getGlobalUsername()+" "+globalPrompt+" ", message)
	}
}

func send(receiverName string, messageText string) (string, bool) {
	if (getGlobalWsConnection() == nil) || (getGlobalUsername() == "") {
		return "Please login to send messages!", false
	}
	var dataDict = make(map[string]string)
	dataDict["userName"] = getGlobalUsername()
	dataDict["password"] = getGlobalPassword()
	dataDict["receiverName"] = receiverName
	dataDict["messageText"] = messageText
	jsonByteArr, jsonErr := json.Marshal(dataDict)
	if jsonErr != nil {
		return "Application error occured during json encoding!", false
	}
	err := getGlobalWsConnection().WriteMessage(websocket.TextMessage, jsonByteArr)
	if err != nil {
		updateGlobalUserNameAndPassword("", "")
		updateGlobalWsConnection(nil)
		return "Failed to send message!", false
	}
	return "", true
}

func main() {

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome! Please Enter Your Commands Below:")
	for {
		fmt.Print(getGlobalUsername() + " " + globalPrompt + " ")
		command, err := reader.ReadString('\n')
		if err != nil {
			panic(err)
		}
		command = strings.TrimSpace(command)
		commandParts := strings.Split(command, " ")
		if commandParts[0] == "signup" {
			if len(commandParts) < 3 {
				fmt.Println("Error! Signup command format: signup <userName> <password>")
			} else {
				var userName string = commandParts[1]
				var password string = commandParts[2]
				msg, _ := signup(userName, password)
				fmt.Println(msg)
			}
		} else if commandParts[0] == "signin" {
			if len(commandParts) < 3 {
				fmt.Println("Error! Signin command format: signin <userName> <password>")
			} else {
				var userName string = commandParts[1]
				var password string = commandParts[2]
				msg, isSuccessful := signin(userName, password)
				if isSuccessful == true {
					newConnection, _, err := websocket.DefaultDialer.Dial(globalServerURLWS+"/wsconnection", nil)
					updateGlobalWsConnection(newConnection)
					if err != nil {
						fmt.Println("Unable to connect to chat server! Sign In failed!", err)
					} else {
						fmt.Println("Successfully established websocket connection with the server!")
						updateGlobalUserNameAndPassword(userName, password)
						_, isSuccessful := send(randomReceiverName, "")
						if isSuccessful == false {
							updateGlobalUserNameAndPassword("", "")
							getGlobalWsConnection().Close()
							fmt.Println("Unable to connect to chat server! Sign In failed!", err)
						} else {
							go receiveMessages()
							fmt.Println("Successfully signed in!")
						}
					}
				} else {
					fmt.Println(msg)
				}
			}
		} else if commandParts[0] == "signout" {
			err := getGlobalWsConnection().Close()
			if err != nil {
				fmt.Println("Unable to sign out:", err, "!")
			} else {
				updateGlobalUserNameAndPassword("", "")
				fmt.Println("Successfully signed out!")
			}
		} else if commandParts[0] == "send" {
			if len(commandParts) < 3 {
				fmt.Println("Error! Send command format: send <userName> <messageText>")
			} else {
				var receiverName string = commandParts[1]
				var messageText string = strings.Join(commandParts[2:], " ")
				msg, _ := send(receiverName, messageText)
				fmt.Println(msg)
			}
		} else {
			fmt.Println("Invalid command!")
		}
	}
}
