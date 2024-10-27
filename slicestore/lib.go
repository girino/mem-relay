package slicestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

var _ eventstore.Store = (*SliceStore)(nil)

// Define the Index interface
type Index interface {
	AddEvent(evt *nostr.Event)
	RemoveEvent(evt *nostr.Event)
	RetrieveEvents(filter nostr.Filter) []*nostr.Event
	DoesIndexApplyToFilter(filter nostr.Filter) bool
	GetStats() *IndexStats
	GetName() string
}

// needs indexes for
// multiple ids
// multiple authors
// kinds 0,2,3,10002
// kind 6,7 + 1 author
// Tag e
// tag p

// create an index for multiple ids, using the index for ids NewIdIndex() and merging the resulting slices

type SliceStore struct {
	internal []*nostr.Event

	MaxLimit int
	Path     string

	indexes []Index

	// stats
	stats IndexStats
}

type KindAuthor struct {
	Kind   int
	Author string
}

type IndexStats struct {
	Count     int
	Runcount  int
	Maxtime   time.Duration
	Mintime   time.Duration
	TotalTime time.Duration
}

// Define the GenericIndex struct
type GenericIndex[K comparable] struct {
	index                  map[K][]*nostr.Event
	mu                     sync.RWMutex
	stats                  IndexStats
	name                   string
	doesIndexApplyToFilter func(filter nostr.Filter) bool
	getKey                 func(evt *nostr.Event) K
	getKeyFilter           func(filter nostr.Filter) []K
}

// NewGenericIndex creates a new GenericIndex
// func NewGenericIndex[K comparable](name string) *GenericIndex[K] {
// 	return &GenericIndex[K]{
// 		index: make(map[K][]*nostr.Event),
// 		name:  name,
// 	}
// }

func NewIdIndex() *GenericIndex[string] {
	return &GenericIndex[string]{
		index: make(map[string][]*nostr.Event),
		name:  "ID",
		getKey: func(evt *nostr.Event) string {
			return evt.ID
		},
		getKeyFilter: func(filter nostr.Filter) []string {
			return filter.IDs
		},
		doesIndexApplyToFilter: func(filter nostr.Filter) bool {
			return len(filter.IDs) >= 1
		},
	}
}

func NewKindAuthorIndex() *GenericIndex[KindAuthor] {
	return &GenericIndex[KindAuthor]{
		index: make(map[KindAuthor][]*nostr.Event),
		name:  "KindAuthor",
		getKey: func(evt *nostr.Event) KindAuthor {
			return KindAuthor{Kind: evt.Kind, Author: evt.PubKey}
		},
		getKeyFilter: func(filter nostr.Filter) []KindAuthor {
			return []KindAuthor{{Kind: filter.Kinds[0], Author: filter.Authors[0]}}
		},
		doesIndexApplyToFilter: func(filter nostr.Filter) bool {
			return len(filter.Kinds) == 1 && len(filter.Authors) == 1
		},
	}
}

func NewKindIndex() *GenericIndex[int] {
	return &GenericIndex[int]{
		index: make(map[int][]*nostr.Event),
		name:  "Kind",
		getKey: func(evt *nostr.Event) int {
			return evt.Kind
		},
		getKeyFilter: func(filter nostr.Filter) []int {
			return filter.Kinds
		},
		doesIndexApplyToFilter: func(filter nostr.Filter) bool {
			if len(filter.Kinds) < 1 {
				return false
			}
			for _, kind := range filter.Kinds {
				// full scan if contains 1, 5 or 7, the most common events
				if kind == 1 || kind == 5 || kind == 7 {
					return false
				}
			}
			return true
		},
	}
}

func NewAuthorIndex() *GenericIndex[string] {
	return &GenericIndex[string]{
		index: make(map[string][]*nostr.Event),
		name:  "Author",
		getKey: func(evt *nostr.Event) string {
			return evt.PubKey
		},
		getKeyFilter: func(filter nostr.Filter) []string {
			return filter.Authors
		},
		doesIndexApplyToFilter: func(filter nostr.Filter) bool {
			return len(filter.Authors) >= 1
		},
	}
}

func (gi *GenericIndex[K]) GetName() string {
	return gi.name
}

func (gi *GenericIndex[K]) GetStats() *IndexStats {
	return &gi.stats
}

func (gi *GenericIndex[K]) DoesIndexApplyToFilter(filter nostr.Filter) bool {
	if gi.doesIndexApplyToFilter == nil {
		return false
	}
	return gi.doesIndexApplyToFilter(filter)
}

func (gi *GenericIndex[K]) GetKey(evt *nostr.Event) K {
	return gi.getKey(evt)
}

// AddEvent adds an event to the index in a sorted way
func (gi *GenericIndex[K]) AddEvent(evt *nostr.Event) {
	gi.mu.Lock()
	defer gi.mu.Unlock()

	key := gi.getKey(evt)

	events, exists := gi.index[key]
	if !exists {
		events = []*nostr.Event{}
	}

	idx, found := slices.BinarySearchFunc(events, evt, eventComparator)
	if found {
		return
	}
	// let's insert at the correct place in the array
	events = append(events, evt) // bogus
	copy(events[idx+1:], events[idx:])
	events[idx] = evt

	gi.index[key] = events
}

// RemoveEvent removes an event from the index
func (gi *GenericIndex[K]) RemoveEvent(evt *nostr.Event) {
	gi.mu.Lock()
	defer gi.mu.Unlock()

	key := gi.getKey(evt)
	events, exists := gi.index[key]
	if !exists {
		return
	}

	idx, found := slices.BinarySearchFunc(events, evt, eventComparator)
	if !found {
		// we don't have this event
		return
	}

	// we have it
	copy(events[idx:], events[idx+1:])
	events = events[0 : len(events)-1]

	// Update the map
	if len(events) == 0 {
		delete(gi.index, key)
	} else {
		gi.index[key] = events
	}
}

// RetrieveEvents retrieves events based on a filter
func (gi *GenericIndex[K]) RetrieveEvents(filter nostr.Filter) []*nostr.Event {
	gi.mu.RLock()
	defer gi.mu.RUnlock()

	keys := gi.getKeyFilter(filter)
	events := make([]*nostr.Event, 0)
	for _, key := range keys {
		if keyEvents, exists := gi.index[key]; exists {
			events = mergeSortedSlices(events, keyEvents, eventComparator)
		}
	}

	return events
}

func (b *SliceStore) Init() error {
	b.internal = make([]*nostr.Event, 0, 5000)
	// declare a static array
	b.indexes = createIndexes()

	if b.MaxLimit == 0 {
		b.MaxLimit = 500
	}
	if b.Path == "" {
		return fmt.Errorf("path not defined")
	}

	// Load events from disk
	if err := b.LoadEventsFromDisk(b.Path); err != nil {
		// retry with backup file
		fmt.Printf("Error loading events from disk: %v\n", err)
		// if backup exists
		if _, err := os.Stat(b.Path + ".bak"); err == nil {
			fmt.Println("Retrying with backup file")
			if err := b.LoadEventsFromDisk(b.Path + ".bak"); err != nil {
				fmt.Printf("Error loading events from backup: %v\n", err)
			}
		}
	}
	// retry with backup file

	// Launch a goroutine to save events to disk every 30 minutes
	go func() {
		ticker1 := time.NewTicker(5 * time.Minute)
		defer ticker1.Stop()
		ticker2 := time.NewTicker(10 * time.Minute)
		defer ticker2.Stop()
		for {
			select {
			case <-ticker2.C:
				if err := b.SaveEventsToDisk(); err != nil {
					fmt.Printf("Error saving events to disk: %v\n", err)
				}
			case <-ticker1.C:
				// print the stats
				printStats("Main", b.stats)
				// print the stats of the indexes
				for _, index := range b.indexes {
					printStats(index.GetName(), *index.GetStats())
				}
				// print the slice size
				fmt.Printf("Slice size: %d\n", len(b.internal))
			}
		}
	}()

	return nil
}

// LoadEventsFromDisk loads events from a JSON file at the specified path
func (b *SliceStore) LoadEventsFromDisk(filename string) error {
	fmt.Printf("Loading events from disk: %s\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// If the file does not exist, it's not an error; just return
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&b.internal); err != nil {
		return err
	}

	// Rebuild the index
	b.indexes = createIndexes()

	for _, evt := range b.internal {
		for _, index := range b.indexes {
			index.AddEvent(evt)
		}
	}

	fmt.Printf("Loaded %d events\n", len(b.internal))
	return nil
}

func createIndexes() []Index {
	return []Index{
		NewIdIndex(),
		NewKindAuthorIndex(),
		NewAuthorIndex(),
		NewKindIndex(),
	}
}

// SaveEventsToDisk saves all events to a JSON file at the specified path
func (b *SliceStore) SaveEventsToDisk() error {
	startTime := time.Now()

	// copy current file to b.Path + ".bak"
	if _, err := os.Stat(b.Path); err == nil {
		if err := os.Rename(b.Path, b.Path+".bak"); err != nil {
			return err
		}
	}

	file, err := os.Create(b.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(b.internal); err != nil {
		return err
	}

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("SaveEventsToDisk took %s\n", elapsedTime)

	return nil
}

func (b *SliceStore) Close() {}

func (b *SliceStore) QueryEvents(ctx context.Context, filter nostr.Filter) (chan *nostr.Event, error) {
	startTime := time.Now()

	ch := make(chan *nostr.Event)
	if filter.Limit > b.MaxLimit || filter.Limit == 0 {
		filter.Limit = b.MaxLimit
	}

	events, stats := b.getEventsSlice(filter)
	if events == nil {
		close(ch)
		updateStats(stats, startTime, 0)
		return ch, nil
	}

	go func() {
		count := 0
		for _, event := range events {
			if count == filter.Limit {
				break
			}

			if filter.Matches(event) {
				select {
				case ch <- event:
				case <-ctx.Done():
					updateStats(stats, startTime, count)
					return
				}
				count++
			}
		}
		close(ch)
		updateStats(stats, startTime, count)
	}()
	return ch, nil
}

func (b *SliceStore) getEventsSlice(filter nostr.Filter) ([]*nostr.Event, *IndexStats) {
	events := b.internal
	stats := &b.stats
	// isFilter := false

	for _, index := range b.indexes {
		if index.DoesIndexApplyToFilter(filter) {
			events = index.RetrieveEvents(filter)
			stats = index.GetStats()
			// isFilter = true
			break
		}
	}

	// if !isFilter {
	// 	fmt.Println("No index found for filter")
	// 	// print the filter
	// 	fmt.Printf("Filter: %+v\n", filter)
	// }

	start := 0
	end := len(events)
	if filter.Until != nil {
		start, _ = slices.BinarySearchFunc(events, *filter.Until, eventTimestampComparator)
	}
	if filter.Since != nil {
		end, _ = slices.BinarySearchFunc(events, *filter.Since, eventTimestampComparator)
	}

	if end < start {
		return nil, stats
	}

	events = events[start:end]
	return events, stats
}

func (b *SliceStore) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var val int64
	events, _ := b.getEventsSlice(filter)
	for _, event := range events {
		if filter.Matches(event) {
			val++
		}
	}
	return val, nil
}

func (b *SliceStore) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	idx, found := slices.BinarySearchFunc(b.internal, evt, eventComparator)
	if found {
		return eventstore.ErrDupEvent
	}
	// let's insert at the correct place in the array
	b.internal = append(b.internal, evt) // bogus
	copy(b.internal[idx+1:], b.internal[idx:])
	b.internal[idx] = evt

	// update the index
	for _, index := range b.indexes {
		index.AddEvent(evt)
	}

	return nil
}

func (b *SliceStore) DeleteEvent(ctx context.Context, evt *nostr.Event) error {
	idx, found := slices.BinarySearchFunc(b.internal, evt, eventComparator)
	if !found {
		// we don't have this event
		return nil
	}

	// we have it
	copy(b.internal[idx:], b.internal[idx+1:])
	b.internal = b.internal[0 : len(b.internal)-1]

	// update the index
	for _, index := range b.indexes {
		index.RemoveEvent(evt)
	}

	return nil
}

func eventTimestampComparator(e *nostr.Event, t nostr.Timestamp) int {
	return int(t) - int(e.CreatedAt)
}

func eventComparator(a *nostr.Event, b *nostr.Event) int {
	c := int(b.CreatedAt) - int(a.CreatedAt)
	if c != 0 {
		return c
	}
	return strings.Compare(b.ID, a.ID)
}

// a function that receives an eventstats and a function, meausres the running time of the function and updates the eventstats
func updateStats(stats *IndexStats, startTime time.Time, increment int) {
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	stats.TotalTime += elapsedTime
	stats.Count += increment
	stats.Runcount++
	if elapsedTime > stats.Maxtime {
		stats.Maxtime = elapsedTime
	}
	if elapsedTime < stats.Mintime {
		stats.Mintime = elapsedTime
	}
}

// a function that print the stats in one line, receives the name of the stats and the stats as params
func printStats(name string, stats IndexStats) {
	//calculate the average time
	var averageTime time.Duration
	if stats.Count > 0 {
		averageTime = stats.TotalTime / time.Duration(stats.Count)
	}
	var averagePerRun time.Duration
	if stats.Runcount > 0 {
		averagePerRun = stats.TotalTime / time.Duration(stats.Runcount)
	}
	// also print the average time
	fmt.Printf("%s:\n"+
		"          : Count:         %d\n"+
		"          : RunCount:      %d\n"+
		"          : Maxtime:       %s\n"+
		"          : Mintime:       %s\n"+
		"          : AverageTime:   %s\n"+
		"          : AveragePerRun: %s\n"+
		"          : TotalTime:     %s\n",
		name, stats.Count, stats.Runcount, stats.Maxtime.String(),
		stats.Mintime.String(), averageTime.String(),
		averagePerRun.String(), stats.TotalTime.String())
}

// mergeSortedSlices merges two sorted slices of events
func mergeSortedSlices(a, b []*nostr.Event, eventComparator func(a *nostr.Event, b *nostr.Event) int) []*nostr.Event {
	result := make([]*nostr.Event, 0, len(a)+len(b))
	i, j := 0, 0
	la, lb := len(a), len(b)
	for i < la && j < lb {
		cmp := eventComparator(a[i], b[j])
		if cmp < 0 {
			result = append(result, a[i])
			i++
		} else if cmp == 0 {
			result = append(result, a[i])
			i++
			j++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	for i < la {
		result = append(result, a[i])
		i++
	}
	for j < lb {
		result = append(result, b[j])
		j++
	}
	return result
}
