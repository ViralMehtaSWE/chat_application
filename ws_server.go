package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var mutexUserNameToMapConnections = &sync.Mutex{}
var mutexConnectionToChannel = &sync.Mutex{}
var mutexUserNameToPassword = &sync.Mutex{}
var mutex = &sync.Mutex{}

var userNameToMapConnections = make(map[string]map[*websocket.Conn]bool)
var connectionToChannel = make(map[*websocket.Conn]chan string)
var userNameToPassword = make(map[string]string)

var maxChannelSize uint32 = 10
var addr = flag.String("addr", "localhost:8080", "http service address")

func getFromUserNameToPassword(key string) (string, bool) {
	mutexUserNameToPassword.Lock()
	value, isPresent := userNameToPassword[key]
	mutexUserNameToPassword.Unlock()
	return value, isPresent
}

func putIntoUserNameToPassword(key string, value string) {
	mutexUserNameToPassword.Lock()
	userNameToPassword[key] = value
	mutexUserNameToPassword.Unlock()
	return
}

func eraseFromUserNameToPassword(key string) {
	mutexUserNameToPassword.Lock()
	delete(userNameToPassword, key)
	mutexUserNameToPassword.Unlock()
	return
}

var upgrader = websocket.Upgrader{} // use default options

func authenticate(r *http.Request) (string, int, bool) {
	var message string
	var statusCode int
	var isSuccessful bool

	byteBodyArr, _ := ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(byteBodyArr))
	var dataDict = make(map[string]string)
	json.Unmarshal(byteBodyArr, &dataDict)
	userName, _ := dataDict["userName"]
	password, _ := dataDict["password"]
	storedPassword, isPresent := getFromUserNameToPassword(userName)

	if (isPresent == false) || ((isPresent == true) && (storedPassword != password)) {
		jsonResponseByteArr, jsonErr := json.Marshal("Wrong username or password!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			message = "json encode error occured"
			statusCode = http.StatusInternalServerError
			isSuccessful = false
			return message, statusCode, isSuccessful
		}
		log.Print(string(jsonResponseByteArr))
		message = string(jsonResponseByteArr)
		statusCode = http.StatusForbidden
		isSuccessful = false
		return message, statusCode, isSuccessful
	}

	jsonResponseByteArr, jsonErr := json.Marshal("Authentication Successful!")
	if jsonErr != nil {
		log.Print("json encode error occured:", jsonErr)
		message = "json encode error occured"
		statusCode = http.StatusInternalServerError
		isSuccessful = false
		return message, statusCode, isSuccessful
	}
	log.Print(string(jsonResponseByteArr))
	message = string(jsonResponseByteArr)
	statusCode = http.StatusForbidden
	isSuccessful = true
	return message, statusCode, isSuccessful
}

func verifyRequestSanity(r *http.Request) (string, int, bool) {
	var message string
	var statusCode int
	var isSuccessful bool
	if r.Method != http.MethodPost {
		jsonResponseByteArr, jsonErr := json.Marshal("Please send a POST request!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			message = "json encode error occured"
			statusCode = http.StatusInternalServerError
			isSuccessful = false
			return message, statusCode, isSuccessful
		}
		log.Print(string(jsonResponseByteArr))
		message = string(jsonResponseByteArr)
		statusCode = http.StatusBadRequest
		isSuccessful = false
		return message, statusCode, isSuccessful
	}

	if r.Body == nil {
		jsonResponseByteArr, jsonErr := json.Marshal("Request body cannot be nil!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			message = "json encode error occured"
			statusCode = http.StatusInternalServerError
			isSuccessful = false
			return message, statusCode, isSuccessful
		}
		log.Print(string(jsonResponseByteArr))
		message = string(jsonResponseByteArr)
		statusCode = http.StatusBadRequest
		isSuccessful = false
		return message, statusCode, isSuccessful
	}

	byteBodyArr, _ := ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(byteBodyArr))
	var dataDict = make(map[string]string)
	jsonErr := json.Unmarshal(byteBodyArr, &dataDict)
	if jsonErr != nil {
		log.Print("json decode error occured:", jsonErr)
		message = "json decode error occured"
		statusCode = http.StatusInternalServerError
		isSuccessful = false
		return message, statusCode, isSuccessful
	}

	_, isPresent := dataDict["userName"]
	if isPresent == false {
		jsonResponseByteArr, jsonErr := json.Marshal("Request body must contain userName field!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			message = "json encode error occured"
			statusCode = http.StatusInternalServerError
			isSuccessful = false
			return message, statusCode, isSuccessful
		}
		log.Print(string(jsonResponseByteArr))
		message = string(jsonResponseByteArr)
		statusCode = http.StatusBadRequest
		isSuccessful = false
		return message, statusCode, isSuccessful
	}

	_, isPresent = dataDict["password"]
	if isPresent == false {
		jsonResponseByteArr, jsonErr := json.Marshal("Request body must contain password field!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			message = "json encode error occured"
			statusCode = http.StatusInternalServerError
			isSuccessful = false
			return message, statusCode, isSuccessful
		}
		log.Print(string(jsonResponseByteArr))
		message = string(jsonResponseByteArr)
		statusCode = http.StatusBadRequest
		isSuccessful = false
		return message, statusCode, isSuccessful
	}

	message = "Request sanity OK"
	statusCode = http.StatusOK
	isSuccessful = true
	log.Print(message)
	return message, statusCode, isSuccessful
}

func eraseFromUserNameToMapConnections(userName string, ws *websocket.Conn) {
	mutexUserNameToMapConnections.Lock()
	defer mutexUserNameToMapConnections.Unlock()
	mp := userNameToMapConnections[userName]
	delete(mp, ws)
	if len(mp) == 0 {
		delete(userNameToMapConnections, userName)
	}
}

func eraseFromConnectionToChannel(ws *websocket.Conn) {
	mutexConnectionToChannel.Lock()
	defer mutexConnectionToChannel.Unlock()
	close(connectionToChannel[ws])
	delete(connectionToChannel, ws)
}

func cleanupAfterConnectionClose(userName string, ws *websocket.Conn) {
	mutex.Lock()
	defer mutex.Unlock()
	eraseFromUserNameToMapConnections(userName, ws)
	eraseFromConnectionToChannel(ws)
	ws.Close()
}

func addNewChannelToConnectionToChannel(ws *websocket.Conn) {
	mutexConnectionToChannel.Lock()
	defer mutexConnectionToChannel.Unlock()
	connectionToChannel[ws] = make(chan string, maxChannelSize)
}

func addNewConnectionToUserNameToMapConnections(userName string, ws *websocket.Conn) {
	mutexUserNameToMapConnections.Lock()
	defer mutexUserNameToMapConnections.Unlock()
	_, isPresent := userNameToMapConnections[userName]
	if isPresent == false {
		userNameToMapConnections[userName] = make(map[*websocket.Conn]bool)
	}
	userNameToMapConnections[userName][ws] = true
}

func pushDataIntoAllChannelsForUser(userName string, data string) {
	mutex.Lock()
	defer mutex.Unlock()
	mutexUserNameToMapConnections.Lock()
	defer mutexUserNameToMapConnections.Unlock()
	mutexConnectionToChannel.Lock()
	defer mutexConnectionToChannel.Unlock()
	mp, isPresent := userNameToMapConnections[userName]
	if isPresent == false || mp == nil {
		return
	}
	for conn, _ := range mp {
		connectionToChannel[conn] <- data
	}
}

func getChannel(ws *websocket.Conn) (chan string, bool) {
	mutexConnectionToChannel.Lock()
	defer mutexConnectionToChannel.Unlock()
	ch, isPresent := connectionToChannel[ws]
	return ch, isPresent
}

func checkIfUserConnected(userName string) bool {
	mutexUserNameToMapConnections.Lock()
	defer mutexUserNameToMapConnections.Unlock()
	mp, isPresent := userNameToMapConnections[userName]
	if isPresent == false {
		return false
	}
	if mp == nil {
		return false
	}
	if len(mp) == 0 {
		return false
	}
	return true
}

func wsConnection(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool {
		return true
	}

	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		jsonResponseByteArr, jsonErr := json.Marshal("Upgrade to websocket connection failed!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Print("Upgrade to websocket connection failed!", err)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, string(jsonResponseByteArr))
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var globalUserName string = ""

	go func(connection *websocket.Conn) {
		for {
			_, message, err := connection.ReadMessage()
			if err != nil {
				log.Println("Error occured while reading message:", err)
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			var dataDict = make(map[string]string)
			jsonErr := json.Unmarshal(message, &dataDict)
			if jsonErr != nil {
				log.Println("json decode error:", jsonErr)
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			userName, isPresent := dataDict["userName"]
			if isPresent == false {
				log.Println("Malformed data, no userName")
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			if userName == "" {
				log.Println("userName cannot be an empty string")
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			password, isPresent := dataDict["password"]
			if isPresent == false {
				log.Println("Malformed data, no password")
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			storedPassword, isPresent := getFromUserNameToPassword(userName)
			if isPresent == false {
				log.Println("User: " + userName + " has not signed up yet")
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			if storedPassword != password {
				log.Println("Wrong userName or password!")
				if globalUserName == "" {
					wg.Done()
				}
				break
			}

			if globalUserName == "" {
				globalUserName = userName
				addNewConnectionToUserNameToMapConnections(userName, connection)
				addNewChannelToConnectionToChannel(connection)
				defer cleanupAfterConnectionClose(userName, connection)
				wg.Done()
				continue
			}

			receiverName, isPresent := dataDict["receiverName"]
			if isPresent == false {
				pushDataIntoAllChannelsForUser(userName, "Malformed data, no receiverName")
				log.Println("Malformed data, no receiverName")
				continue
			}

			if receiverName == "" {
				pushDataIntoAllChannelsForUser(userName, "receiverName cannot be an empty string")
				log.Println("User: " + receiverName + " does not exist!")
				continue
			}

			storedPassword, isPresent = getFromUserNameToPassword(receiverName)
			if isPresent == false {
				pushDataIntoAllChannelsForUser(userName, "User: "+receiverName+" has not signed up yet")
				log.Println("User: " + receiverName + " has not signed up yet")
				continue
			}

			isConnected := checkIfUserConnected(receiverName)
			if isConnected == false {
				pushDataIntoAllChannelsForUser(userName, "User: "+receiverName+" is offline!")
				log.Println("User: " + receiverName + " is offline!")
				continue
			}

			messageText, isPresent := dataDict["messageText"]
			if isPresent == false {
				pushDataIntoAllChannelsForUser(userName, "No messageText found")
				log.Println("No messageText found")
				continue
			}

			pushDataIntoAllChannelsForUser(userName, userName+" -> "+receiverName+": "+messageText)
			pushDataIntoAllChannelsForUser(receiverName, userName+" -> "+receiverName+": "+messageText)

			log.Println("Received message from "+userName+": ", dataDict)
		}
	}(connection)

	wg.Wait()
	msgsToSend, isPresent := getChannel(connection)
	if isPresent == true {
		var isClosed bool = false
		for data := range msgsToSend {
			if isClosed == false {
				err = connection.WriteMessage(websocket.TextMessage, []byte(data))
				if err != nil {
					log.Println("write error:", err)
					isClosed = true
				}
			}
		}
	}
	fmt.Println("Exiting ...")
}

func signup(w http.ResponseWriter, r *http.Request) {
	message, statusCode, isSuccessful := verifyRequestSanity(r)
	if isSuccessful == false {
		w.WriteHeader(statusCode)
		fmt.Fprintf(w, message)
		return
	}
	byteBodyArr, _ := ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(byteBodyArr))
	fmt.Println("In signup>", string(byteBodyArr))
	var dataDict = make(map[string]string)
	json.Unmarshal(byteBodyArr, &dataDict)
	userName, _ := dataDict["userName"]
	password, _ := dataDict["password"]

	_, isPresent := getFromUserNameToPassword(userName)

	if isPresent == true {
		jsonResponseByteArr, jsonErr := json.Marshal("You have already signed up before!")
		if jsonErr != nil {
			log.Print("json encode error occured:", jsonErr)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Print("You have already signed up before!")
		w.WriteHeader(http.StatusMethodNotAllowed)
		fmt.Fprintf(w, string(jsonResponseByteArr))
		return
	}

	jsonResponseByteArr, jsonErr := json.Marshal("Signup Successful!")
	if jsonErr != nil {
		log.Print("json encode error occured:", jsonErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	putIntoUserNameToPassword(userName, password)
	fmt.Fprintf(w, string(jsonResponseByteArr))
}

func signin(w http.ResponseWriter, r *http.Request) {
	message, statusCode, isSuccessful := verifyRequestSanity(r)
	if isSuccessful == false {
		w.WriteHeader(statusCode)
		fmt.Fprintf(w, message)
		return
	}

	message, statusCode, isSuccessful = authenticate(r)
	if isSuccessful == false {
		w.WriteHeader(statusCode)
		fmt.Fprintf(w, message)
		return
	}

	jsonResponseByteArr, jsonErr := json.Marshal("Signin Successful!")
	if jsonErr != nil {
		log.Print("json encode error occured:", jsonErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, string(jsonResponseByteArr))
}

func main() {
	flag.Parse()
	log.SetFlags(0)
	http.HandleFunc("/wsconnection", wsConnection)
	http.HandleFunc("/signup", signup)
	http.HandleFunc("/signin", signin)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
