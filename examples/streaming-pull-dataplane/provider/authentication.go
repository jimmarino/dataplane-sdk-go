//  Copyright (c) 2025 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
//       Metaform Systems, Inc. - initial API and implementation
//

package provider

import (
	"errors"
	"fmt"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/streaming-pull-dataplane/config"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/synadia-io/callout.go"
	"log"
	"time"
)

// AuthService implements a NATS authentication callout extension. For details, see:
// https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_callout.
type AuthService struct {
	tokenStore  *common.Store[storeEntry]
	authService *callout.AuthorizationService
	nc          *nats.Conn
	accountKeys nkeys.KeyPair
}

func NewAuthService() *AuthService {
	accountKeys, _ := nkeys.CreateAccount()
	return &AuthService{tokenStore: common.NewStore[storeEntry](), accountKeys: accountKeys}
}

func (as *AuthService) InvalidateToken(processId string) error {
	// Remove from token store
	found := as.tokenStore.Delete(processId)
	if !found {
		return fmt.Errorf("token not found")
	}
	return nil
}

func (as *AuthService) Init() error {
	authKeyPair, _ := nkeys.FromSeed([]byte(authKP))

	authorizer := func(req *jwt.AuthorizationRequest) (string, error) {
		token := req.ConnectOptions.Token
		userClaims, err := jwt.DecodeUserClaims(token)
		if err != nil {
			log.Printf("Error decoding user JWT: %v\n", err)
		}

		// use the server-specified user nkey
		uc := jwt.NewUserClaims(req.UserNkey)

		// put the user in the global account
		uc.Audience = "$G"
		uc.Name = userClaims.Name // sets to processid
		uc.Pub.Allow.Add("_INBOX.>")
		uc.Expires = time.Now().Unix() + 90

		if as.tokenStore.Has(userClaims.Name) {
			return uc.Encode(authKeyPair)
		}
		return "", errors.New("not authorized")
	}

	// create a connection using the callout user
	nc, err := nats.Connect(natsUrl, nats.UserInfo("auth", "pass"))
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	as.nc = nc

	authLogger := logger.NewStdLogger(true, false, false, true, false)
	as.authService, err = callout.NewAuthorizationService(nc,
		callout.Authorizer(authorizer),
		callout.ResponseSignerKey(authKeyPair),
		callout.Logger(authLogger))

	if err != nil {
		return err
	}

	return nil
}

func (ns *AuthService) CreateToken(processId string) (string, error) {
	userKeys, _ := nkeys.CreateUser()
	userPKey, _ := userKeys.PublicKey()

	userClaims := jwt.NewUserClaims(userPKey)
	userClaims.Name = processId

	// Restrict permissions to publish only to the forward and response subjects
	userClaims.Permissions.Sub.Allow.Add(processId + config.ForwardSuffix)
	userClaims.Permissions.Sub.Allow.Add(processId + config.ReplySuffix)

	userJWT, _ := userClaims.Encode(ns.accountKeys)
	ns.tokenStore.Create(processId, storeEntry{processId, userJWT})
	return userJWT, nil
}

func (ac *AuthService) shutdown() {
	if ac.nc != nil {
		ac.nc.Close()
	}
}

type storeEntry struct {
	processId string
	token     string
}
