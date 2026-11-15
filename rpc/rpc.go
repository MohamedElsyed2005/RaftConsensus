// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex

	// new methods
	stopping bool
	stopDone chan struct{}
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
// Start begins accepting remote calls at address. Use port 0 to request an available port.
func (s *Server) Start(address string) error {
	s.mu.Lock() // mu -> sync.Mutex 
	defer s.mu.Unlock()
    
	// 3.Reject a second Start call while the server is already running.
	if s.listener != nil || s.stopping {
		return errors.New("server is already running or stopping")
	}
    
	// 1. Create a TCP listener for the requested address. 
	listener, err := net.Listen("tcp", address)
	// 2. Return the listener creation error if startup fails.
	if err != nil {
		return fmt.Errorf("start server: %w", err)
	}
    
	// 4. Save the listener in the server.
	s.listener = listener 
	// 5. Save the server lifecycle state needed by Stop.  
	s.stopped = make(chan struct{})
	s.acceptDone = make(chan struct{})
    
	// 6. Start accepting connections in a background goroutine.
	go s.acceptLoop(listener, s.stopped, s.acceptDone)
    
	// 7. Return without waiting for a client
	return nil
}

// New implementation 
func (s *Server) acceptLoop(
	listener net.Listener,
	stopped <-chan struct{},
	done chan<- struct{},
) {
	defer close(done)

	for {
		connection, err := listener.Accept()
		if err != nil {
			select {
			case <-stopped:
				return
			default:
				return
			}
		}

		s.mu.Lock()

		select {
		case <-stopped:
			s.mu.Unlock()
			_ = connection.Close()
			return
		default:
			s.workers.Add(1)
		}

		s.mu.Unlock()

		go func(conn net.Conn) {
			defer s.workers.Done()
			s.handle(conn)
		}(connection)
	}
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
func (s *Server) Address() string {
	// 5. Read shared server state safely while other goroutines may be running.
	s.mu.Lock()
	defer s.mu.Unlock()
    
	// 3. Return an empty string before the server starts.
	// 4. Return an empty string after the server stops.
	if s.listener == nil || s.stopping {
		return ""
	}
    
	// 1. Return the actual address of the active listener.
	// 2. Return the operating-system-selected port when Start used port 0.
	return s.listener.Addr().String()
}

// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.
func (s *Server) Stop() error {
	s.mu.Lock()
    
	// 7. Be safe to call before Start or more than once.
	if s.listener == nil {
		if s.stopping {
			done := s.stopDone
			s.mu.Unlock()
			<-done
		} else {
			s.mu.Unlock()
		}
		return nil
	}

	if s.stopping {
		done := s.stopDone
		s.mu.Unlock()
		<-done
		return nil
	}
    
	// 1. Stop accepting new connections.
	s.stopping = true
	s.stopDone = make(chan struct{})

	listener := s.listener
	// 3. Signal the accept loop to stop. 
	stopped := s.stopped
	// 4. Wait for the accept loop to finish. 
	acceptDone := s.acceptDone
	stopDone := s.stopDone
    // 3. Signal the accept loop to stop. 
	close(stopped)
	// 2. Close the listener.
	err := listener.Close()

	s.mu.Unlock()

	<-acceptDone
	// 5. Wait for active connection handlers to finish.
	s.workers.Wait()
    
	// 6. Leave the server ready for a later Start call.
	s.mu.Lock()
	s.listener = nil
	s.stopped = nil
	s.acceptDone = nil
	s.stopping = false
	close(stopDone)
	s.stopDone = nil
	s.mu.Unlock()
     
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("stop server: %w", err)
	}

	return nil
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
func (s *Server) handle(connection net.Conn) {
	// 8. Close the connection when handling finishes.
	defer connection.Close()

	// Bound the whole exchange so a silent or slow client cannot
	// keep this worker (and Stop) waiting forever.
	// 9. A bad request must not crash the server or leave the client waiting forever. 
	_ = connection.SetDeadline(time.Now().Add(defaultCallTimeout))

	encoder := json.NewEncoder(connection)
	// 7. Convert a method panic into an error response.
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()
    
	// 1. Read one JSON request from the connection. 
	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+1)))
	var req request
	// 2. Return an error response for malformed JSON.
	if err := decoder.Decode(&req); err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("invalid request: %v", err)}) // 6. Return a JSON error response when the method fails. 
		return
	}
	// 3. Validate the request. 
	if err := validateRequest(req); err != nil {
		_ = encoder.Encode(response{Error: err.Error()}) // 6. Return a JSON error response when the method fails. 
		return
	}
	// 4. Invoke the requested method. 
	result, err := s.invoke(req)
	if err != nil {
		_ = encoder.Encode(response{Error: err.Error()}) // 6. Return a JSON error response when the method fails. 
		return
	}
	// 5. Return a successful JSON response when the method succeeds.
	payload, err := json.Marshal(result)
	if err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("encode result: %v", err)}) // 6. Return a JSON error response when the method fails. 
		return
	}
	_ = encoder.Encode(response{OK: true, Value: payload})
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.

// 1. Implement validateRequest. 
func validateRequest(req request) error {
	// 2. The function must reject requests when:
	// 2.1. The method name is empty. 
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}
    // 2.2. The method name contains whitespace or control characters.
	for _, char := range req.Method {
		if unicode.IsSpace(char) || unicode.IsControl(char) {
			return errors.New(
				"method name cannot contain whitespace or control characters",
			)
		}
	}
    // 2.3. The number of arguments is greater than maxArguments. 
	if len(req.Args) > maxArguments {
		return fmt.Errorf(
			"too many arguments: maximum is %d",
			maxArguments,
		)
	}

	totalSize := 0
    // 2.4. An individual encoded argument is greater than maxArgumentBytes.
	for index, arg := range req.Args {
		if len(arg) > maxArgumentBytes {
			return fmt.Errorf(
				"argument %d is larger than the maximum allowed size",
				index,
			)
		}

		totalSize += len(arg)
	}
    // 2.5. The combined encoded arguments are greater than maxRequestBytes. 
	if totalSize > maxRequestBytes {
		return errors.New("request arguments are larger than the maximum allowed size")
	}

	return nil
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	// 1. Find the requested exported method using reflection.
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	// 2. Return a clear error when the method does not exist.
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	// 3. Check the argument count. 
	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	// 4. Decode each JSON argument into the method parameter's Go type. 
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	// 5. Call the method with the decoded arguments.
	// 8. Convert a method panic into an error. in callMethodSafely()
	outputs, err := callMethodSafely(method, arguments) 
	if err != nil {
		return nil, err
	}
	// 6. Support the method return shapes listed above.
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		// 7. Return a non-nil method error as a remote error.
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	// 6. Support the method return shapes listed above.
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.

func (c *Client) Call(method string, out any, args ...any) error {
	// 1. Reject an empty method name.
	if method == "" {
		return errors.New("method cannot be empty") // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}

	if c.Address == "" {
		return errors.New("client address cannot be empty") // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}
    // 3. Apply the configured timeout as a connection and I/O deadline.
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultCallTimeout
	}

	// Convert the arguments into JSON before sending them.
	// 4. Encode and send one request.
	rawArgs := make([]json.RawMessage, len(args))
	for i, arg := range args {
		data, err := json.Marshal(arg)
		if err != nil {
			return fmt.Errorf("encode argument %d: %w", i, err) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
		}
		rawArgs[i] = data
	}

	req := request{
		Method: method,
		Args:   rawArgs,
	}

	// Connect with a timeout.
	// 2. Open one TCP connection to the server.
	connection, err := net.DialTimeout("tcp", c.Address, timeout)
	if err != nil {
		return fmt.Errorf("connect to server: %w", err) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}
	// 9. Close the connection correctly.
	defer connection.Close()

	// Apply a deadline to both reading and writing.
	if err := connection.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("set connection deadline: %w", err) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}

	// Send the request.
	if err := json.NewEncoder(connection).Encode(req); err != nil {
		return fmt.Errorf("send request: %w", err) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}

	// Read the response.
	// 5. Read and decode one response.
	var resp response
	if err := json.NewDecoder(connection).Decode(&resp); err != nil {
		return fmt.Errorf("read response: %w", err) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}

	// The server reports remote method errors in the response.
	// 6. Return the server's error when the call fails remotely.
	if !resp.OK {
		if resp.Error == "" {
			return errors.New("remote call failed without an error message") // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
		}
		return errors.New(resp.Error) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}

	// A nil output means the caller does not need a return value.
	// 8. Accept nil for out when the remote method has no result.  
	if out == nil {
		return nil
	}

	// Decode the returned value into the caller's output variable.
	// 7. Decode the returned value into out.
	if err := json.Unmarshal(resp.Value, out); err != nil {
		return fmt.Errorf("decode response: %w", err) // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
	}

	return nil  // 10. Return useful errors for connection, encoding, decoding, timeout, and output-type failures. 
}
