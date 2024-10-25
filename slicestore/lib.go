package slicestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fiatjaf/eventstore"
	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/exp/slices"
)

var _ eventstore.Store = (*SliceStore)(nil)

type SliceStore struct {
	internal []*nostr.Event

	MaxLimit int
	Path     string

	indexId         *map[string]*nostr.Event
	indexKindAuthor *map[KindAuthor][]*nostr.Event

	// stats
	stats           IndexStats
	idstats         IndexStats
	kindauthorstats IndexStats
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

func (b *SliceStore) Init() error {
	b.internal = make([]*nostr.Event, 0, 5000)
	b.indexId = &map[string]*nostr.Event{}
	b.indexKindAuthor = &map[KindAuthor][]*nostr.Event{}

	if b.MaxLimit == 0 {
		b.MaxLimit = 500
	}
	if b.Path == "" {
		return fmt.Errorf("path not defined")
	}

	// Load events from disk
	if err := b.LoadEventsFromDisk(); err != nil {
		fmt.Printf("Error loading events from disk: %v\n", err)
	}

	// Launch a goroutine to save events to disk every 30 minutes
	go func() {
		ticker1 := time.NewTicker(30 * time.Second)
		defer ticker1.Stop()
		ticker2 := time.NewTicker(3 * time.Minute)
		defer ticker2.Stop()
		for {
			select {
			case <-ticker2.C:
				if err := b.SaveEventsToDisk(); err != nil {
					fmt.Printf("Error saving events to disk: %v\n", err)
				}
			case <-ticker1.C:
				// print the stats
				printStats("Main      ", b.stats)
				printStats("ID        ", b.idstats)
				printStats("KindAuthor", b.kindauthorstats)
				// print the slice size
				fmt.Printf("Slice size: %d\n", len(b.internal))
			}
		}
	}()

	return nil
}

// LoadEventsFromDisk loads events from a JSON file at the specified path
func (b *SliceStore) LoadEventsFromDisk() error {
	file, err := os.Open(b.Path)
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
	*b.indexId = map[string]*nostr.Event{}
	*b.indexKindAuthor = map[KindAuthor][]*nostr.Event{}
	for _, evt := range b.internal {
		(*b.indexId)[evt.ID] = evt
		b.AddEventToIndex(evt)
	}

	return nil
}

// SaveEventsToDisk saves all events to a JSON file at the specified path
func (b *SliceStore) SaveEventsToDisk() error {
	startTime := time.Now()

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
	ch := make(chan *nostr.Event)
	if filter.Limit > b.MaxLimit || filter.Limit == 0 {
		filter.Limit = b.MaxLimit
	}

	// if index can be used, use it
	if len(filter.IDs) == 1 {
		go measureTime(&b.idstats, func() int {
			count := 0
			if evt, ok := (*b.indexId)[filter.IDs[0]]; ok {
				if filter.Matches(evt) {
					ch <- evt
					count++
				}
			}
			close(ch)
			return count
		})
		return ch, nil
	}

	if len(filter.Kinds) == 1 && len(filter.Authors) == 1 {
		go measureTime(&b.kindauthorstats, func() int {
			count := 0
			if events, ok := (*b.indexKindAuthor)[KindAuthor{Kind: filter.Kinds[0], Author: filter.Authors[0]}]; ok {
				for _, evt := range events {
					if count >= filter.Limit {
						break
					}
					count++
					if filter.Matches(evt) {
						count++
						select {
						case ch <- evt:
						case <-ctx.Done():
							return count
						}
					}
				}
			}
			close(ch)
			return count
		})
		return ch, nil
	}

	// efficiently determine where to start and end
	start := 0
	end := len(b.internal)
	if filter.Until != nil {
		start, _ = slices.BinarySearchFunc(b.internal, *filter.Until, eventTimestampComparator)
	}
	if filter.Since != nil {
		end, _ = slices.BinarySearchFunc(b.internal, *filter.Since, eventTimestampComparator)
	}

	// ham
	if end < start {
		close(ch)
		return ch, nil
	}

	go measureTime(&b.stats, func() int {
		count := 0
		for _, event := range b.internal[start:end] {
			if count == filter.Limit {
				break
			}

			if filter.Matches(event) {
				select {
				case ch <- event:
				case <-ctx.Done():
					return 0
				}
				count++
			}
		}
		close(ch)
		return count
	})
	return ch, nil
}

func (b *SliceStore) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	var val int64
	for _, event := range b.internal {
		if filter.Matches(event) {
			val++
		}
	}
	return val, nil
}

// AddEventToIndex adds an event to the indexKindAuthor map in a sorted way
func (b *SliceStore) AddEventToIndex(evt *nostr.Event) {
	key := KindAuthor{Kind: evt.Kind, Author: evt.PubKey}
	events, exists := (*b.indexKindAuthor)[key]
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

	(*b.indexKindAuthor)[key] = events
}

// RemoveEventFromIndex removes an event from the indexKindAuthor map
func (b *SliceStore) RemoveEventFromIndex(evt *nostr.Event) {
	key := KindAuthor{Kind: evt.Kind, Author: evt.PubKey}
	events, exists := (*b.indexKindAuthor)[key]
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
		delete((*b.indexKindAuthor), key)
	} else {
		(*b.indexKindAuthor)[key] = events
	}
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
	(*b.indexId)[evt.ID] = evt
	b.AddEventToIndex(evt)

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
	delete(*b.indexId, evt.ID)
	b.RemoveEventFromIndex(evt)

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
func measureTime(stats *IndexStats, f func() int) {
	startTime := time.Now()
	increment := f()
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
	fmt.Printf("%s: Count: %5d, RunCount: %5d, Maxtime: %-9s, Mintime: %-9s, AverageTime: %-9s, AveragePerRun %-9s\n", name, stats.Count, stats.Runcount, stats.Maxtime.String(), stats.Mintime.String(), averageTime.String(), averagePerRun.String())
}
