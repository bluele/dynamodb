package dynamodb

import (
	"errors"
	"fmt"

	simplejson "github.com/bitly/go-simplejson"
)

func (t *Table) Query(attributeComparisons []AttributeComparison, isRetry bool) ([]map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	return RunQuery(q, t, isRetry)
}

func (t *Table) QueryOnIndex(attributeComparisons []AttributeComparison, indexName string, isRetry bool) ([]map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	q.AddIndex(indexName)
	return RunQuery(q, t, isRetry)
}

func (t *Table) LimitedQuery(attributeComparisons []AttributeComparison, limit int64, isRetry bool) ([]map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	q.AddLimit(limit)
	return RunQuery(q, t, isRetry)
}

func (t *Table) LimitedQueryOnIndex(attributeComparisons []AttributeComparison, indexName string, limit int64, isRetry bool) ([]map[string]*Attribute, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	q.AddIndex(indexName)
	q.AddLimit(limit)
	return RunQuery(q, t, isRetry)
}

func (t *Table) CountQuery(attributeComparisons []AttributeComparison, isRetry bool) (int64, error) {
	q := NewQuery(t)
	q.AddKeyConditions(attributeComparisons)
	q.AddSelect("COUNT")
	jsonResponse, err := t.Server.queryServer("DynamoDB_20120810.Query", q, isRetry)
	if err != nil {
		return 0, err
	}
	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return 0, err
	}

	itemCount, err := json.Get("Count").Int64()
	if err != nil {
		return 0, err
	}

	return itemCount, nil
}

func (t *Table) RawQueryTable(query string, target string, isRetry bool) ([]map[string]*Attribute, *Key, error) {
	var retryCount = 0
	if !isRetry {
		retryCount = -1
	}
	jsonResponse, err := t.Server.rawQueryServer("DynamoDB_20120810."+target, query, retryCount)
	if err != nil {
		return nil, nil, err
	}

	json, err := simplejson.NewJson(jsonResponse)
	if err != nil {
		return nil, nil, err
	}

	itemCount, err := json.Get("Count").Int()
	if err != nil {
		if target == "UpdateItem" {
			return make([]map[string]*Attribute, 0), nil, nil
		}

		message := fmt.Sprintf("Unexpected response %s", jsonResponse)
		return nil, nil, errors.New(message)
	}

	results := make([]map[string]*Attribute, itemCount)

	for i, _ := range results {
		item, err := json.Get("Items").GetIndex(i).Map()
		if err != nil {
			message := fmt.Sprintf("Unexpected response %s", jsonResponse)
			return nil, nil, errors.New(message)
		}
		results[i] = parseAttributes(item)
	}

	var lastEvaluatedKey *Key
	if lastKeyMap := json.Get("LastEvaluatedKey").MustMap(); lastKeyMap != nil {
		lastEvaluatedKey = parseKey(t, lastKeyMap)
	}

	return results, lastEvaluatedKey, nil
}

func (t *Table) QueryTable(q *Query, isRetry bool) ([]map[string]*Attribute, *Key, error) {
	return t.RawQueryTable(q.String(), "Query", isRetry)
}

func RunQuery(q *Query, t *Table, isRetry bool) ([]map[string]*Attribute, error) {
	result, _, err := t.QueryTable(q, isRetry)
	if err != nil {
		return nil, err
	}

	return result, err

}
