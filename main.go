package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/fiatjaf/eventstore"

	"github.com/fiatjaf/khatru"
	//"github.com/girino/eventstore/lmdb"

	"gitea.girino.org/girino/nostr-relay/slicestore"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

var (
	config *Config
)

type Config struct {
	Relays                 []string `json:"relays"`
	WriteRelays            []string `json:"writeRelays"`
	DbPath                 string   `json:"dbPath"`
	BlacklistPath          string   `json:"blacklistPath"`
	RelayName              string   `json:"relayName"`
	NSec                   string   `json:"nSec"`
	MinDifficulty          int      `json:"minDifficulty"`
	WotPubkey              string   `json:"wotPubkey"`
	WotDepth               int      `json:"wotDepth"`
	WoTMinimumFollowers    int      `json:"wotMinimumFollowers"`
	WotRefrreshTimeSeconds int      `json:"wotRefrreshTimeSeconds"`
}

type RelayInfo struct {
	RelayName   string
	RelayURL    string
	AddressNpub string
	AddressHex  string
}

type RelayPointers struct {
	relay           *khatru.Relay
	store           *eventstore.RelayWrapper
	acceptEventFunc func(event *nostr.Event) bool
	db              *slicestore.SliceStore
}

func main() {
	// Set memory limit (e.g., 500 MiB)
	const memoryLimit = 2048 * 1024 * 1024 // 2GB
	debug.SetMemoryLimit(memoryLimit)

	// main context, should be used to cancel any background goroutines
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// disable logging of errors from relay?
	nostr.InfoLogger = log.New(io.Discard, "", 0)
	// read config from json
	var err error
	config, err = readConfigsFromJSON("config.json")
	if err != nil {
		log.Println("error reading config file:", err)
		panic(err)
	}

	// Create a channel for events
	eventChannel := make(chan *nostr.Event, 100)

	relayMap := make(map[string]RelayPointers)
	mainDescription := config.RelayName + ": Main Relay"
	relayMap["main"] = createRelay("main", mainDescription, eventChannel, func(event *nostr.Event) bool {
		return true
	})
	// relayMap["main"] = createRelay("main", mainDescription, eventChannel, BlockReplyGuysAcceptEvent)
	// nip13Description := config.RelayName + ": NIP-13 Relay with minimum difficulty of " + strconv.Itoa(config.MinDifficulty)
	// relayMap["nip13"] = createRelay("nip13", nip13Description, eventChannel, func(event *nostr.Event) bool {
	// 	return ValidatePoW(event, config.MinDifficulty)
	// })
	// relayMap["wot"] = CreateWotRelay(ctx, "wot", config.RelayName+": Web of Trust Relay", eventChannel)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dynamicRelayHandler(w, r, relayMap)
	})

	// go restartableBackgroundFunction(ctx, func(parentContext context.Context) {
	// 	copyFromUpstream(parentContext, relayMap)
	// })
	go copyFromUpstream(ctx, relayMap)

	// // send to upstream relays ust in case
	// // Start the consumer goroutine
	// go restartableBackgroundFunction(ctx, func(parentContext context.Context) {
	// 	consumeSavedEvents(parentContext, eventChannel)
	// })
	go consumeSavedEvents(ctx, eventChannel)

	// monitoring resources
	go monitorResources(ctx)

	// Handle interrupt signal (Ctrl+C)
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChannel
		log.Println("Received interrupt signal, shutting down...")
		cancel()
		for _, relay := range relayMap {
			if relay.db != nil {
				relay.db.SaveEventsToDisk()
			}
			relay.store.Store.Close()
			relay.store.Close()
		}
		os.Exit(0)
	}()

	log.Println("running on :3335")
	err = http.ListenAndServe(":3335", handler)
	if err != nil {
		log.Println("error starting server:", err)
	}
}

func monitorResources(parentContext context.Context) {
	var m runtime.MemStats
	for {
		select {
		case <-parentContext.Done():
			log.Println("Parent context canceled. Exiting memory stats monitoring goroutine.")
			return
		default:
			log.Printf("Number of Goroutines: %d", runtime.NumGoroutine())
			runtime.ReadMemStats(&m)
			log.Printf("Alloc = %v MiB, Sys = %v MiB, NumGC = %v",
				m.Alloc/1024/1024,
				m.Sys/1024/1024,
				m.NumGC)
			log.Println("🫂 network size:", len(pubkeyFollowerCount))
			time.Sleep(30 * time.Second)
		}
	}
}

func consumeSavedEvents(ctx context.Context, eventChannel <-chan *nostr.Event) {
	myCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var relays []*nostr.Relay
	for _, relayURL := range config.WriteRelays {
		relay, err := nostr.RelayConnect(myCtx, relayURL)
		if err != nil {
			log.Println("error connecting to relay:", err)
			continue
		}
		relays = append(relays, relay)
	}

	recentlySeen := NewLimitedMap(10)

	for {
		select {
		case event := <-eventChannel:
			if event == nil {
				continue
			}
			if _, ok := recentlySeen.Get(event.ID); ok {
				continue
			}
			recentlySeen.Add(event.ID, true)
			for i := 0; i < len(relays); i++ {
				// try reconnecting in case of disconnect
				relay := relays[i]
				var err error
				if !relay.IsConnected() {
					log.Println("reconnecting to relay:", relay.URL)
					relay.Close()
					relay, err = nostr.RelayConnect(myCtx, relay.URL)
					if err != nil {
						log.Println("error connecting to relay:", err)
						continue
					}
					log.Println("reconnected to relay:", relay.URL)
					relays[i] = relay
				}
				// log.Println("publishing event ", event.ID, " to relay:", relay.URL)
				err = relay.Publish(myCtx, *event)
				if err != nil {
					log.Println("error publishing event:", err)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func dynamicRelayHandler(w http.ResponseWriter, r *http.Request, subRelays map[string]RelayPointers) {
	var subdomain string = "main"
	dotIndex := strings.Index(r.Host, ".")
	if dotIndex != -1 {
		subdomain = r.Host[:dotIndex]
	}
	if _, ok := subRelays[subdomain]; !ok {
		if _, ok := subRelays["main"]; !ok {
			panic("no relay found for main")
		}
		subRelays[subdomain] = subRelays["main"]
	}
	subRelays[subdomain].relay.ServeHTTP(w, r)
}

func extractPubKey() (string, string) {
	_, privKey, err := nip19.Decode(config.NSec)
	if err != nil {
		panic(err)
	}

	pubKey, err := nostr.GetPublicKey(privKey.(string))
	if err != nil {
		panic(err)
	}
	addressNpub, err := nip19.EncodePublicKey(pubKey)
	if err != nil {
		panic(err)
	}

	return pubKey, addressNpub
}

func createRelay(relayName string, description string, eventChannel chan<- *nostr.Event, acceptEventFunc func(event *nostr.Event) bool) RelayPointers {
	relay := khatru.NewRelay()

	addressHex, addressNpub := extractPubKey()

	relay.Info.Name = config.RelayName + ": " + relayName
	relay.Info.PubKey = addressHex
	relay.Info.Description = description

	// db := sqlite3.SQLite3Backend{DatabaseURL: config.DbPath + "/" + relayName + "/" + relayName + ".db"}
	// db = badger.BadgerBackend{Path: config.DbPath + "/" + relayName}
	// db := lmdb.LMDBBackend{Path: config.DbPath + "/" + relayName, MapSize: 1 << 34}
	db := slicestore.SliceStore{Path: config.DbPath + "/" + "slice.json"}
	// os.MkdirAll(config.DbPath+"/"+relayName, 0755)
	if err := db.Init(); err != nil {
		panic(err)
	}
	wdb := eventstore.RelayWrapper{Store: &db}
	relay.StoreEvent = append(relay.StoreEvent, db.SaveEvent)
	relay.QueryEvents = append(relay.QueryEvents, db.QueryEvents)
	relay.CountEvents = append(relay.CountEvents, db.CountEvents)
	relay.DeleteEvent = append(relay.DeleteEvent, db.DeleteEvent)
	relay.RejectEvent = append(relay.RejectEvent, func(ctx context.Context, event *nostr.Event) (bool, string) {
		if acceptEventFunc(event) {
			return false, ""
		}
		return true, "blocked: you are not allowed to post to this relay"
	})

	// some to rates, etc.
	// policies.ApplySaneDefaults(relay)

	// send to upstream
	relay.OnEventSaved = append(relay.OnEventSaved, func(ctx context.Context, event *nostr.Event) {
		eventChannel <- event
	})

	mux := relay.Router()
	static := http.FileServer(http.Dir("static"))
	mux.Handle("GET /static/", http.StripPrefix("/static/", static))
	mux.Handle("GET /favicon.ico", http.StripPrefix("/", static))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tmpl := template.Must(template.ParseFiles("templates/index.html"))
		data := RelayInfo{
			RelayName:   description,
			RelayURL:    getFullURL(r),
			AddressNpub: addressNpub,
			AddressHex:  addressHex,
		}
		err := tmpl.Execute(w, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	return RelayPointers{relay, &wdb, acceptEventFunc, &db}
}

func getFullURL(r *http.Request) string {
	// scheme := "http"
	// if r.TLS != nil {
	scheme := "https" // force https
	// }
	return fmt.Sprintf("%s://%s%s", scheme, r.Host, r.URL.RequestURI())
}

func copyFromUpstream(parentContext context.Context, relays map[string]RelayPointers) {
	ctx, cancel := context.WithCancel(parentContext)
	defer cancel()

	pool := nostr.NewSimplePool(ctx)
	since := nostr.Now()

	filters := []nostr.Filter{{
		Kinds: []int{
			nostr.KindArticle,
			nostr.KindDeletion,
			nostr.KindContactList,
			nostr.KindEncryptedDirectMessage,
			nostr.KindMuteList,
			nostr.KindReaction,
			nostr.KindRelayListMetadata,
			nostr.KindRepost,
			nostr.KindZapRequest,
			nostr.KindZap,
			nostr.KindTextNote,
			nostr.KindProfileMetadata,
			nostr.KindRelayListMetadata,
		},
		Since: &since,
	}}
	for ev := range pool.SubMany(ctx, config.Relays, filters) {
		for _, r := range relays {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if r.acceptEventFunc(ev.Event) {
				if len(ev.Event.Tags) > math.MaxUint16 || len(ev.Event.Content) > math.MaxUint16 {
					log.Println("event too big, skipping")
					continue
				}
				// calls as a function so we can defer the cancel
				func() {
					eventCtx, innercancel := context.WithTimeout(ctx, r.relay.WriteWait)
					defer innercancel()
					err := (*r.store).Publish(eventCtx, *ev.Event)
					if err != nil && err.Error() != "failed to save: UNIQUE constraint failed: event.id" {
						log.Println("error publishing event:", err)
					}
					r.relay.BroadcastEvent(ev.Event)
				}()
			}
		}
	}
}

// func restartableBackgroundFunction(parentContext context.Context, funcToRun func(pc context.Context)) {
// 	var panicCount int
// 	var firstPanicTime time.Time

// 	for {
// 		select {
// 		case <-parentContext.Done():
// 			log.Println("Parent context canceled. Exiting restartableBackgroundFunction.")
// 			return
// 		default:
// 		}
// 		func() {
// 			defer func() {
// 				if r := recover(); r != nil {
// 					log.Println("Recovered from panic:", r)
// 					panicCount++
// 					if panicCount == 1 {
// 						firstPanicTime = time.Now()
// 					}
// 					if panicCount > 3 && time.Since(firstPanicTime) <= 1*time.Minute {
// 						log.Println("Too many panics in a short time. Exiting application.")
// 						os.Exit(1)
// 					}
// 					if time.Since(firstPanicTime) > 1*time.Minute {
// 						panicCount = 1
// 						firstPanicTime = time.Now()
// 					}
// 				}
// 			}()
// 			funcToRun(parentContext)
// 		}()
// 		select {
// 		case <-parentContext.Done():
// 			log.Println("Parent context canceled. Exiting restartableBackgroundFunction.")
// 			return
// 		default:
// 			log.Println("Restarting Go routine in 10 secs")
// 			time.Sleep(10 * time.Second) // Optional: Add a delay before restarting
// 		}
// 	}
// }

func readConfigsFromJSON(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		return nil, err
	}
	if config.MinDifficulty < 0 || config.MinDifficulty > 255 {
		return nil, fmt.Errorf("MinDifficulty must be between 0 and 255")
	} else if config.MinDifficulty == 0 {
		config.MinDifficulty = 5 // defaults to 5
	}

	if strings.HasPrefix(config.WotPubkey, "npub") {
		_, addressHex, err := nip19.Decode(config.WotPubkey)
		if err != nil {
			return nil, err
		}
		config.WotPubkey = addressHex.(string)
	}
	if len(config.WotPubkey) != 64 {
		return nil, fmt.Errorf("WotPubkey must be a 64 character hex string")
	}

	if config.WotDepth < 1 {
		config.WotDepth = 3 // defaults to 3
	}

	if config.WoTMinimumFollowers <= 0 {
		config.WoTMinimumFollowers = 1 // defaults to 1
	}

	if config.WotRefrreshTimeSeconds <= 0 {
		config.WotRefrreshTimeSeconds = (3600 * 3) // defaults to 3 hours
	}

	return &config, nil
}
