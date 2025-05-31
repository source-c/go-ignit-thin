package ignite

import (
	"fmt"
	"github.com/source-c/go-ignit-thin/internal/bitset"
	"strconv"
	"strings"
)

// AttributeFeature defines protocol specific feature flag.
type AttributeFeature uint

const (
	UserAttributesFeature AttributeFeature = iota
	ExecuteTaskByNameFeature
	ClusterStatesFeature
	ClusterGroupGetNodesEndpointsFeature
	ClusterGroupsFeature
	ServiceInvokeFeature
	DefaultQueryTimeoutFeature
	QueryPartitionsBatchSizeFeature
	BinaryConfigurationFeature
	GetServiceDescriptorsFeature
	ServiceInvokeCallContextFeature
	HeartbeatFeature
	DataReplicationOperationsFeature
	AllAffinityMappingsFeature
	IndexQueryFeature
	IndexQueryLimitFeature
	ServiceTopologyFeature
	_minFeature = UserAttributesFeature
	_maxFeature = ServiceTopologyFeature
)

// ProtocolVersion defines a thin client protocol verison.
type ProtocolVersion struct {
	Major int16 // Major version number.
	Minor int16 // Minor version number
	Patch int16 // Patch version number
}

// ProtocolContext is a set of current version and features flags.
type ProtocolContext struct {
	version  ProtocolVersion
	features *bitset.BitSet
}

// NewProtocolContext creates a new protocol context.
func NewProtocolContext(version ProtocolVersion, features ...AttributeFeature) *ProtocolContext {
	ctx := ProtocolContext{
		version: version,
	}

	if ctx.SupportsBitmapFeatures() {
		ctx.features = bitset.New()
		for _, feature := range features {
			ctx.features.Set(uint(feature))
		}
	}

	return &ctx
}

// Version returns a thin client protocol version
func (ctx *ProtocolContext) Version() ProtocolVersion {
	return ctx.version
}

func (ctx *ProtocolContext) marshal(writer BinaryOutputStream) {
	writer.WriteInt16(ctx.version.Major)
	writer.WriteInt16(ctx.version.Minor)
	writer.WriteInt16(ctx.version.Patch)
	writer.WriteInt8(2)
	if ctx.SupportsBitmapFeatures() {
		marshalByteArray(writer, ctx.features.Bytes())
	}
}

func (ctx *ProtocolContext) updateAttributeFeatures(bs *bitset.BitSet) {
	if bs != nil {
		ctx.features = bs
	}
}

// SupportsAttributeFeature checks whether the context supports specific feature.
func (ctx *ProtocolContext) SupportsAttributeFeature(f AttributeFeature) bool {
	if ctx.features == nil {
		return false
	}
	return ctx.features.Test(uint(f))
}

func (ctx *ProtocolContext) SupportsAuthorization() bool {
	return ctx.version.Compare(ProtocolVersion{1, 1, 0}) >= 0
}

func (ctx *ProtocolContext) SupportsQueryEntityPrecisionAndScale() bool {
	return ctx.version.Compare(ProtocolVersion{1, 2, 0}) >= 0
}

func (ctx *ProtocolContext) SupportsPartitionAwareness() bool {
	return ctx.version.Compare(ProtocolVersion{1, 4, 0}) >= 0
}

func (ctx *ProtocolContext) SupportsTransactions() bool {
	return ctx.version.Compare(ProtocolVersion{1, 5, 0}) >= 0
}

func (ctx *ProtocolContext) SupportsExpiryPolicy() bool {
	return ctx.version.Compare(ProtocolVersion{1, 6, 0}) >= 0
}

func (ctx *ProtocolContext) SupportsClusterApi() bool {
	return ctx.version.Compare(ProtocolVersion{1, 6, 0}) >= 0
}

func (ctx *ProtocolContext) SupportsBitmapFeatures() bool {
	return ctx.version.Compare(ProtocolVersion{1, 7, 0}) >= 0
}

// ParseVersion parses [ProtocolVersion] from string, returns version and true if succeeded, otherwise returns invalid version and false.
func ParseVersion(ver string) (ProtocolVersion, bool) {
	res := ProtocolVersion{}
	parts := strings.Split(ver, ".")
	if len(parts) != 3 {
		return res, false
	}
	for i, part := range parts {
		var curr int64
		var err error
		if curr, err = strconv.ParseInt(part, 10, 16); err != nil {
			return res, false
		}
		if i == 0 {
			res.Major = int16(curr)
		} else if i == 1 {
			res.Minor = int16(curr)
		} else {
			res.Patch = int16(curr)
		}
	}
	return res, true
}

func (curr *ProtocolVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", curr.Major, curr.Minor, curr.Patch)
}

// Compare the version to the other. Returns 0 if equals, positive integer if the version is greater than the other, otherwise returns negative integer.
func (curr *ProtocolVersion) Compare(other ProtocolVersion) int {
	if diff := curr.Major - other.Major; diff != 0 {
		return int(diff)
	}

	if diff := curr.Minor - other.Minor; diff != 0 {
		return int(diff)
	}

	return int(curr.Patch - other.Patch)
}
