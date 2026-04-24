/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubernetes

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"

	kitmd "github.com/dapr/kit/metadata"
)

const (
	defaultLeaseNamePrefix = "dapr-lock-"
	// Keep room for a 40-char hash suffix under the 253-char DNS-1123 subdomain limit.
	maxLeaseNamePrefixLen = 200
)

type kubernetesMetadata struct {
	// Namespace in which Lease objects are created. If unset, the NAMESPACE
	// environment variable is used.
	Namespace string `mapstructure:"namespace"`

	// Optional path to a kubeconfig file. When empty, falls back to in-cluster
	// config and then to the default kubeconfig location.
	KubeconfigPath string `mapstructure:"kubeconfigPath"`

	// Prefix prepended to the hashed ResourceID when naming Lease objects.
	// Must be a valid DNS-1123 subdomain prefix.
	LeaseNamePrefix string `mapstructure:"leaseNamePrefix"`
}

func (m *kubernetesMetadata) parse(properties map[string]string) error {
	if err := kitmd.DecodeMetadata(properties, m); err != nil {
		return err
	}

	if m.LeaseNamePrefix == "" {
		m.LeaseNamePrefix = defaultLeaseNamePrefix
	}
	if len(m.LeaseNamePrefix) > maxLeaseNamePrefixLen {
		return fmt.Errorf("leaseNamePrefix is too long (max %d characters)", maxLeaseNamePrefixLen)
	}
	// The prefix plus a lowercase hex suffix must form a DNS-1123 subdomain.
	// Validate the prefix alone with a placeholder suffix to catch bad characters early.
	if errs := validation.IsDNS1123Subdomain(m.LeaseNamePrefix + "0"); len(errs) > 0 {
		return fmt.Errorf("leaseNamePrefix %q is not a valid Kubernetes resource name prefix: %s", m.LeaseNamePrefix, strings.Join(errs, "; "))
	}

	if m.Namespace != "" {
		if errs := validation.IsDNS1123Label(m.Namespace); len(errs) > 0 {
			return fmt.Errorf("namespace %q is not a valid Kubernetes namespace: %s", m.Namespace, strings.Join(errs, "; "))
		}
	}

	return nil
}

func (m *kubernetesMetadata) validateResolvedNamespace() error {
	if m.Namespace == "" {
		return errors.New("namespace is required: set the 'namespace' metadata property or the NAMESPACE environment variable")
	}
	return nil
}
