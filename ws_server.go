package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

/*
Database Tables:

 CREATE TABLE Authentication (
    username varchar(100) NOT NULL,
    password varchar(100) NOT NULL,
    PRIMARY KEY (username)
);

CREATE TABLE Messages (
	id int NOT NULL AUTO_INCREMENT,
    timestamp varchar(100) NOT NULL,
    sender varchar(100) NOT NULL,
    receiver varchar(100) NOT NULL,
    message varchar(10000) NOT NULL,
    PRIMARY KEY (id)
);

*/

var maxUserNameLength int = 100
var maxPasswordLength int = 100
var db *sql.DB = nil
var dbUserName string = "XXXX"
var dbPassword string = "XXXX"
var dbName string = "XXXX"
var dbServerIP string = "XXXX"

var mutexUserNameToMapConnections = &sync.Mutex{}
var mutexConnectionToChannel = &sync.Mutex{}
var mutexUserNameToPassword = &sync.Mutex{}
var mutex = &sync.Mutex{}

var userNameToMapConnections = make(map[string]map[*websocket.Conn]bool)
var connectionToChannel = make(map[*websocket.Conn]chan string)
var userNameToPassword = make(map[string]string)

var maxChannelSize uint32 = 10
var addr = flag.String("addr", "0.0.0.0:8080", "http service address")

func dbAddNewUserNameAndPassword(userName string, password string) (string, bool) {
	if len(userName) > maxUserNameLength {
		return "len(userName) should be less than 101!", false
	}
	if len(password) > maxPasswordLength {
		return "len(password) should be less than 101!", false
	}

	stmt, err := db.Prepare("INSERT INTO Authentication (username, password) VALUES ('" + userName + "','" + password + "');")
	if err != nil {
		return err.Error(), false
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err.Error(), false
	}
	return "", true
}

func dbGetPassword(userName string) (string, bool, string, bool) {
	if len(userName) > maxUserNameLength {
		return "", false, "len(userName) should be less than 101!", true
	}

	res, err := db.Query("SELECT password FROM Authentication WHERE username = '" + userName + "';")
	if err != nil {
		return "", false, err.Error(), false
	}
	defer res.Close()
	var password string
	ctr := 0
	for res.Next() {
		err = res.Scan(&password)
		if err != nil {
			return "", false, string(err.Error()), false
		}
		ctr++
	}

	if ctr == 0 {
		return "", false, "", true
	}
	if ctr > 1 {
		return "", false, "More than 1 row with same userName found in the messages table!", false
	}
	return password, true, "", true
}

func dbGetLastNMessages(user1 string, user2 string, n int) ([]string, string, bool) {
	if len(user1) > maxUserNameLength {
		return []string{}, "Both len(senderName) and len(receiverName) should be less than 101!", false
	}
	if len(user2) > maxUserNameLength {
		return []string{}, "Both len(senderName) and len(receiverName) should be less than 101!", false
	}

	res, err := db.Query("SELECT * FROM Messages WHERE ((sender = '" + user1 + "' AND receiver = '" + user2 + "') OR (sender = '" + user2 + "' AND receiver = '" + user1 + "')) ORDER BY id DESC LIMIT " + strconv.Itoa(n) + ";")
	if err != nil {
		return []string{}, err.Error(), false
	}
	defer res.Close()
	var ans = []string{}

	for res.Next() {
		var id int
		var timestamp string
		var sender string
		var receiver string
		var message string
		err = res.Scan(&id, &timestamp, &sender, &receiver, &message)
		if err != nil {
			return []string{}, string(err.Error()), false
		}
		ans = append(ans, timestamp+": "+sender+" -> "+receiver+": "+message)
	}

	var temp = []string{}
	n = len(ans)
	for i := n - 1; i >= 0; i-- {
		temp = append(temp, ans[i])
	}
	ans = temp
	return ans, "", true
}

func dbInsertNewMessage(sender string, receiver string, message string) (string, bool) {
	if len(sender) > maxUserNameLength {
		return "len(sender) should be less than 101!", false
	}

	if len(receiver) > maxUserNameLength {
		return "len(receiver) should be less than 101!", false
	}

	if len(message) > maxUserNameLength {
		return "len(messageText) should be less than 10001!", false
	}

	stmt, err := db.Prepare("INSERT INTO Messages (timestamp, sender, receiver, message) VALUES ('" + time.Now().UTC().String() + "','" + sender + "','" + receiver + "','" + message + "');")
	if err != nil {
		return err.Error(), false
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err.Error(), false
	}
	return "", true
}

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

	if isPresent == false {
		storedPasswordDb, isPresentDb, errMsg, isSuccessful := dbGetPassword(userName)
		if isSuccessful == false {
			message = errMsg
			statusCode = http.StatusInternalServerError
			isSuccessful = false
			return message, statusCode, isSuccessful
		}
		if isPresentDb == true {
			isPresent = true
			storedPassword = storedPasswordDb
			putIntoUserNameToPassword(userName, storedPasswordDb)
		}
	}

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

func addNewConnection(connection *websocket.Conn, userName string) {
	mutex.Lock()
	addNewConnectionToUserNameToMapConnections(userName, connection)
	addNewChannelToConnectionToChannel(connection)
	mutex.Unlock()
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
				storedPasswordDb, isPresentDb, errMsg, isSuccessful := dbGetPassword(userName)
				if isSuccessful == false {
					log.Print(errMsg)
					if globalUserName == "" {
						wg.Done()
					}
					break
				}
				if isPresentDb == true {
					isPresent = true
					storedPassword = storedPasswordDb
					putIntoUserNameToPassword(userName, storedPasswordDb)
				}
			}

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
				addNewConnection(connection, userName)
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
				_, isFound, errMsg, isSuccessful := dbGetPassword(receiverName)
				if isSuccessful == false {
					log.Print(errMsg)
					pushDataIntoAllChannelsForUser(userName, "Internal Server Error!")
				}
				if isFound == true {
					isPresent = true
				}
			}

			if isPresent == false {
				pushDataIntoAllChannelsForUser(userName, "User: "+receiverName+" has not signed up yet")
				log.Println("User: " + receiverName + " has not signed up yet")
				continue
			}

			messageText, isPresent := dataDict["messageText"]
			if isPresent == false {
				pushDataIntoAllChannelsForUser(userName, "No messageText found")
				log.Println("No messageText found")
				continue
			}

			errMsg, isSuccessful := dbInsertNewMessage(userName, receiverName, messageText)
			if isSuccessful == false {
				log.Print(errMsg)
				pushDataIntoAllChannelsForUser(userName, "Internal server error!")
				continue
			}

			isConnected := checkIfUserConnected(receiverName)
			if isConnected == false {
				pushDataIntoAllChannelsForUser(userName, "User: "+receiverName+" is offline, however, "+receiverName+" will be able to see this message later as we have saved your message!")
				log.Println("User: " + receiverName + " is offline, however, " + receiverName + " will be able to see this message later as we have saved your message!")
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

	_, isFound, errMsg, isSuccessful := dbGetPassword(userName)
	if isSuccessful == false {
		log.Print(errMsg)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if isFound == true {
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

	errMsg, isSuccessful = dbAddNewUserNameAndPassword(userName, password)
	if isSuccessful == false {
		log.Print(errMsg)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
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

func getLastNMessages(w http.ResponseWriter, r *http.Request) {
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

	byteBodyArr, _ := ioutil.ReadAll(r.Body)
	r.Body = ioutil.NopCloser(bytes.NewBuffer(byteBodyArr))

	var dataDict = make(map[string]string)
	json.Unmarshal(byteBodyArr, &dataDict)
	userName, _ := dataDict["userName"]
	receiverName, isPresent := dataDict["receiverName"]
	if isPresent == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Request must contain receiverName!")
		return
	}

	N, isPresent := dataDict["N"]
	if isPresent == false {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Request must contain N: The number of most recent messages required!")
		return
	}

	intN, err := strconv.Atoi(N)
	if err != nil {
		log.Print(err.Error())
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "N must be an integer")
		return
	}

	msgs, errMsg, isSuccessful := dbGetLastNMessages(userName, receiverName, intN)
	if isSuccessful == false {
		log.Print(errMsg)
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Internal Server Error!")
		return
	}

	jsonByteArr, err := json.Marshal(msgs)
	if err != nil {
		log.Print(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Json encoding error")
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, string(jsonByteArr))
}

func main() {
	dbcon, err := sql.Open("mysql", dbUserName+":"+dbPassword+"@tcp("+dbServerIP+")/"+dbName)
	if err != nil {
		panic(err.Error())
	}
	db = dbcon
	defer dbcon.Close()
	flag.Parse()
	log.SetFlags(0)
	http.HandleFunc("/wsconnection", wsConnection)
	http.HandleFunc("/signup", signup)
	http.HandleFunc("/signin", signin)
	http.HandleFunc("/getlastnmessages", getLastNMessages)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
