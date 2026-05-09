package hilo

import (
	"context"
	"encoding/json"
	"fmt"
)

// GraphQLRequest is the standard JSON envelope for a GraphQL POST.
type GraphQLRequest struct {
	OperationName string         `json:"operationName,omitempty"`
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables,omitempty"`
}

// GraphQLError is one error entry returned alongside (or instead of) data.
type GraphQLError struct {
	Message    string         `json:"message"`
	Path       []any          `json:"path,omitempty"`
	Extensions map[string]any `json:"extensions,omitempty"`
}

// GraphQLResponse holds the raw `data` payload alongside any errors.
type GraphQLResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []GraphQLError  `json:"errors,omitempty"`
}

// GraphQL posts a raw query to the digital-twin endpoint.
func (c *Client) GraphQL(ctx context.Context, req GraphQLRequest) (*GraphQLResponse, error) {
	var resp GraphQLResponse
	if err := c.Post(ctx, "/api/digital-twin/v3/graphql", req, &resp); err != nil {
		return nil, err
	}
	if len(resp.Errors) > 0 {
		return &resp, fmt.Errorf("graphql: %s", resp.Errors[0].Message)
	}
	return &resp, nil
}
