package dsdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
)

type FlowType string

const (
	Pull FlowType = "pull"
	Push FlowType = "push"
)

type DataAddress struct {
	Properties map[string]any `json:"properties"`
}

func NewDataAddressBuilder() *DataAddressBuilder {
	return &DataAddressBuilder{
		properties: make(map[string]any),
	}
}

type DataAddressBuilder struct {
	properties map[string]any
}

func (b *DataAddressBuilder) Property(key string, value any) *DataAddressBuilder {
	b.properties[key] = value
	return b
}

func (b *DataAddressBuilder) Properties(props map[string]any) *DataAddressBuilder {
	for k, v := range props {
		b.properties[k] = v
	}
	return b
}

func (b *DataAddressBuilder) Build() (*DataAddress, error) {
	if len(b.properties) == 0 {
		return nil, errors.New("properties are required")
	}

	return &DataAddress{
		Properties: b.properties,
	}, nil
}

type TransferType struct {
	DestinationType string   `json:"destinationType"`
	FlowType        FlowType `json:"flowType"`
}

type DataFlowBaseMessage struct {
	// ParticipantContext string
	MessageId              string       `json:"messageId"` // NEW
	ParticipantId          string       `json:"participantId"`
	ProcessId              string       `json:"processId"`
	AgreementId            string       `json:"agreementId"`
	CallbackAddress        CallbackURL  `json:"callbackAddress"`
	TransferType           TransferType `json:"transferType"`
	DestinationDataAddress DataAddress  `json:"destinationDataAddress"`
}

type DataFlowStartMessage struct {
	DataFlowBaseMessage
	SourceDataAddress DataAddress `json:"sourceDataAddress"`
}

type DataFlowProvisionMessage struct {
	DataFlowBaseMessage
}

type DataFlowResponseMessage struct {
	DataplaneId  string      `json:"dataplaneId"`
	DataAddress  DataAddress `json:"dataAddress"`
	Provisioning bool        `json:"provisioning"`
}

type DataFlowState int

const (
	Provisioning DataFlowState = 50
	Received     DataFlowState = 100
	Started      DataFlowState = 150
	Completed    DataFlowState = 200
	Suspended    DataFlowState = 225
	Terminated   DataFlowState = 250
	Failed       DataFlowState = 300
	Notified     DataFlowState = 400
)

type DataFlow struct {
	ID                     string
	RuntimeId              string
	UpdatedAt              int64
	CreatedAt              int64
	ParticipantContextId   string
	DataspaceContext       string
	CounterpartyId         string
	CallbackAddress        CallbackURL
	TransferType           TransferType
	State                  DataFlowState
	StateCount             uint
	StateTimestamp         int64
	SourceDataAddress      DataAddress
	DestinationDataAddress DataAddress
	ErrorDetail            string
}

type DataFlowBuilder struct {
	dataFlow DataFlow
}

func NewDataFlowBuilder() *DataFlowBuilder {
	return &DataFlowBuilder{}
}

func (b *DataFlowBuilder) ID(id string) *DataFlowBuilder {
	b.dataFlow.ID = id
	return b
}

func (b *DataFlowBuilder) UpdatedAt(updatedAt int64) *DataFlowBuilder {
	b.dataFlow.UpdatedAt = updatedAt
	return b
}

func (b *DataFlowBuilder) CreatedAt(createdAt int64) *DataFlowBuilder {
	b.dataFlow.CreatedAt = createdAt
	return b
}

func (b *DataFlowBuilder) ParticipantContextId(id string) *DataFlowBuilder {
	b.dataFlow.ParticipantContextId = id
	return b
}

func (b *DataFlowBuilder) DataspaceContext(context string) *DataFlowBuilder {
	b.dataFlow.DataspaceContext = context
	return b
}

func (b *DataFlowBuilder) CounterpartyId(id string) *DataFlowBuilder {
	b.dataFlow.CounterpartyId = id
	return b
}

func (b *DataFlowBuilder) State(state DataFlowState) *DataFlowBuilder {
	b.dataFlow.State = state
	return b
}

func (b *DataFlowBuilder) StateCount(count uint) *DataFlowBuilder {
	b.dataFlow.StateCount = count
	return b
}

func (b *DataFlowBuilder) StateTimestamp(timestamp int64) *DataFlowBuilder {
	b.dataFlow.StateTimestamp = timestamp
	return b
}

func (b *DataFlowBuilder) SourceDataAddress(address DataAddress) *DataFlowBuilder {
	b.dataFlow.SourceDataAddress = address
	return b
}

func (b *DataFlowBuilder) DestinationDataAddress(address DataAddress) *DataFlowBuilder {
	b.dataFlow.DestinationDataAddress = address
	return b
}

func (b *DataFlowBuilder) CallbackAddress(callback CallbackURL) *DataFlowBuilder {
	b.dataFlow.CallbackAddress = callback
	return b
}

func (b *DataFlowBuilder) TransferType(transferType TransferType) *DataFlowBuilder {
	b.dataFlow.TransferType = transferType
	return b
}

func (b *DataFlowBuilder) ErrorDetail(error string) *DataFlowBuilder {
	b.dataFlow.ErrorDetail = error
	return b
}

func (b *DataFlowBuilder) RuntimeId(id string) *DataFlowBuilder {
	b.dataFlow.RuntimeId = id
	return b
}

func (b *DataFlowBuilder) Build() (*DataFlow, error) {
	var validationErrs []string

	if b.dataFlow.CreatedAt == 0 {
		b.dataFlow.CreatedAt = time.Now().UnixMilli()
	}

	if b.dataFlow.UpdatedAt == 0 {
		b.dataFlow.UpdatedAt = b.dataFlow.CreatedAt
	}

	if b.dataFlow.StateTimestamp == 0 {
		b.dataFlow.StateTimestamp = b.dataFlow.CreatedAt
	}

	if b.dataFlow.ID == "" {
		validationErrs = append(validationErrs, "ID is required")
	}

	if b.dataFlow.ParticipantContextId == "" {
		validationErrs = append(validationErrs, "ParticipantContextId is required")
	}

	if b.dataFlow.DataspaceContext == "" {
		validationErrs = append(validationErrs, "DataspaceContext is required")
	}

	if b.dataFlow.CounterpartyId == "" {
		validationErrs = append(validationErrs, "CounterpartyId is required")
	}

	if b.dataFlow.SourceDataAddress.Properties == nil {
		validationErrs = append(validationErrs, "SourceDataAddress properties are required")
	}

	if b.dataFlow.DestinationDataAddress.Properties == nil {
		validationErrs = append(validationErrs, "DestinationDataAddress properties are required")
	}

	if b.dataFlow.CallbackAddress.URL().String() == "" {
		validationErrs = append(validationErrs, "CallbackAddress is required")
	}

	if b.dataFlow.TransferType.DestinationType == "" {
		validationErrs = append(validationErrs, "TransferType destination type is required")
	}

	if b.dataFlow.TransferType.FlowType == "" {
		validationErrs = append(validationErrs, "TransferType flow type is required")
	}

	if b.dataFlow.RuntimeId == "" {
		validationErrs = append(validationErrs, "RuntimeId is required")
	}

	if len(validationErrs) > 0 {
		return nil, fmt.Errorf("validation failed: %v", validationErrs)
	}

	return &b.dataFlow, nil
}

type CallbackURL url.URL

func (u CallbackURL) MarshalJSON() ([]byte, error) {
	//	Convert to standard url.URL and get the string representation
	stdURL := url.URL(u)
	return json.Marshal(stdURL.String())
}

func (u *CallbackURL) UnmarshalJSON(data []byte) error {
	var urlStr string
	if err := json.Unmarshal(data, &urlStr); err != nil {
		return err
	}

	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return err
	}

	*u = CallbackURL(*parsedURL)
	return nil
}

func (u *CallbackURL) URL() *url.URL {
	if u == nil {
		return nil
	}

	urlCopy := url.URL(*u)
	return &urlCopy
}
