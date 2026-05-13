package minirpc

import "fmt"

type RemoteRequester interface {
	CallRemote(serviceMethod string, args any, reply any) error
}

type Dialer interface {
	Dial(addr string) (RemoteRequester, error)
}

// NotConnectedRequester is a Null Object that handles the uninitialized state.
type NotConnectedRequester struct{}

func (n NotConnectedRequester) CallRemote(serviceMethod string, args any, reply any) error {
	return fmt.Errorf("not connected: please execute 'dial' first")
}
