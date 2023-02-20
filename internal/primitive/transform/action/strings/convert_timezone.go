package strings

import (
	"encoding/json"
	"time"

	"github.com/jmespath/go-jmespath"
)

// ConvertTimezone converts the time in a specific timezone from the source JSON path to time in another timezone,
// and assigns the converted time to the target JSON path.
func ConvertTimezone(input interface{}, sourceJsonPath, fromTimeZone, toTimeZone, targetJsonPath, dateTimeFormat string) (interface{}, error) {
	// Use the default format if no format is specified.
	if dateTimeFormat == "" {
		dateTimeFormat = "2006-01-02 15:04:05"
	}

	// Parse the source JSON path to get the source time value.
	sourceTime, err := jmespath.Search(sourceJsonPath, input)
	if err != nil {
		return nil, err
	}

	// Check if the source time value is a string.
	sourceTimeString, ok := sourceTime.(string)
	if !ok {
		return nil, nil
	}

	// Parse the source time value.
	sourceTimeParsed, err := time.ParseInLocation(dateTimeFormat, sourceTimeString, time.Local)
	if err != nil {
		return nil, err
	}

	// Convert the source time to the target timezone.
	targetTimeParsed := sourceTimeParsed.In(timezoneFromString(fromTimeZone)).UTC()

	// Convert the target time to the destination timezone.
	targetTimeParsed = targetTimeParsed.In(timezoneFromString(toTimeZone))

	// Format the target time value.
	targetTimeString := targetTimeParsed.Format(dateTimeFormat)

	// Update the target JSON path with the target time value.
	return jmespath.Update(input, targetJsonPath, func(interface{}) (interface{}, error) {
		return targetTimeString, nil
	})
}

// timezoneFromString returns a *time.Location corresponding to the given timezone string.
func timezoneFromString(timezone string) *time.Location {
	if timezone == "" {
		return time.UTC
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.UTC
	}
	return location
}
