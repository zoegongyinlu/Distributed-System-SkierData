# DynamoDB Data Model Design

## Table: SkierLiftRides

### Primary Key Structure:
- Partition Key: skierId (Number) - Range from 1 to 100,000
- Sort Key: seasonId_dayId_liftId_time (String) - Composite key for sorting rides
### Attributes:

- `skerId` (number)
- `resortId` (Number)
- `seasonId` (String) 
- `dayId` (String)
- `liftId` (Number)
- `time` (Number)
- `vertical` (Number) - Computed as liftId * 10

### GSI 1: ResortDaySkiers

- Partition Key: resortId (Number)
- Sort Key: seasonId_dayId_skierId (String)

### GSI 2: SkierDaysInSeason

- Partition Key: skierId (Number)
- Sort Key: seasonId_dayId (String)

## Query Support:

1. "For skier N, how many days have they skied this season?"
   - Query base table with Partition Key = skierId
   - Filter for records starting with the desired seasonId
   -  Count distinct seasonId_dayId combinations
2. "For skier N, what are the vertical totals for each ski day?"
   - Query base table with Partition Key = skierId
   - Group by seasonId_dayId (extracted from sort key) and sum vertical values

3. "For skier N, show me the lifts they rode on each ski day"
   - Query base table with Partition Key = skierId
   - Group by seasonId_dayId and collect distinct liftIds

4. "How many unique skiers visited resort X on day N?"
   - Query GSI1 with Partition Key = resortId
   - Filter for sort keys beginning with "seasonId_dayId"
   Count distinct skierId values (extracted from sort key)