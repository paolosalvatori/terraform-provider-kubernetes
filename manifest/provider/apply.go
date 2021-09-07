package provider

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	"github.com/hashicorp/terraform-provider-kubernetes/manifest/morph"
	"github.com/hashicorp/terraform-provider-kubernetes/manifest/payload"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// ApplyResourceChange function
func (s *RawProviderServer) ApplyResourceChange(ctx context.Context, req *tfprotov5.ApplyResourceChangeRequest) (*tfprotov5.ApplyResourceChangeResponse, error) {
	resp := &tfprotov5.ApplyResourceChangeResponse{}

	execDiag := s.canExecute()
	if len(execDiag) > 0 {
		resp.Diagnostics = append(resp.Diagnostics, execDiag...)
		return resp, nil
	}

	rt, err := GetResourceType(req.TypeName)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to determine planned resource type",
			Detail:   err.Error(),
		})
		return resp, nil
	}

	applyPlannedState, err := req.PlannedState.Unmarshal(rt)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to unmarshal planned resource state",
			Detail:   err.Error(),
		})
		return resp, nil
	}
	s.logger.Trace("[ApplyResourceChange][PlannedState] %#v", applyPlannedState)

	applyPriorState, err := req.PriorState.Unmarshal(rt)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to unmarshal prior resource state",
			Detail:   err.Error(),
		})
		return resp, nil
	}
	s.logger.Trace("[ApplyResourceChange]", "[PriorState]", dump(applyPriorState))

	var plannedStateVal map[string]tftypes.Value = make(map[string]tftypes.Value)
	err = applyPlannedState.As(&plannedStateVal)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to extract planned resource state values",
			Detail:   err.Error(),
		})
		return resp, nil
	}

	// Extract computed fields configuration
	computedFields := make(map[string]*tftypes.AttributePath)
	var atp *tftypes.AttributePath
	cfVal, ok := plannedStateVal["computed_fields"]
	if ok && !cfVal.IsNull() && cfVal.IsKnown() {
		var cf []tftypes.Value
		cfVal.As(&cf)
		for _, v := range cf {
			var vs string
			err := v.As(&vs)
			if err != nil {
				s.logger.Error("[computed_fields] cannot extract element from list")
				continue
			}
			atp, err := FieldPathToTftypesPath(vs)
			if err != nil {
				s.logger.Error("[Configure]", "[computed_fields] cannot parse field path element", err)
				resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
					Severity: tfprotov5.DiagnosticSeverityError,
					Summary:  "[computed_fields] cannot parse filed path element: " + vs,
					Detail:   err.Error(),
				})
				continue
			}
			computedFields[atp.String()] = atp
		}
	} else {
		// When not specified by the user, 'metadata.annotations' and 'metadata.labels' are configured as default
		atp = tftypes.NewAttributePath().WithAttributeName("metadata").WithAttributeName("annotations")
		computedFields[atp.String()] = atp

		atp = tftypes.NewAttributePath().WithAttributeName("metadata").WithAttributeName("labels")
		computedFields[atp.String()] = atp
	}

	c, err := s.getDynamicClient()
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics,
			&tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  "Failed to retrieve Kubernetes dynamic client during apply",
				Detail:   err.Error(),
			})
		return resp, nil
	}
	m, err := s.getRestMapper()
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics,
			&tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  "Failed to retrieve Kubernetes RESTMapper client during apply",
				Detail:   err.Error(),
			})
		return resp, nil
	}
	var rs dynamic.ResourceInterface

	switch {
	case applyPriorState.IsNull() || (!applyPlannedState.IsNull() && !applyPriorState.IsNull()):
		// Apply resource
		obj, ok := plannedStateVal["object"]
		if !ok {
			resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  "Failed to find object value in planned resource state",
			})
			return resp, nil
		}

		gvk, err := GVKFromTftypesObject(&obj, m)
		if err != nil {
			return resp, fmt.Errorf("failed to determine resource GVK: %s", err)
		}

		tsch, err := s.TFTypeFromOpenAPI(ctx, gvk, false)
		if err != nil {
			return resp, fmt.Errorf("failed to determine resource type ID: %s", err)
		}

		// "Computed" attributes would have been replaced with Unknown values during
		// planning in order to allow the response from apply to return potentially
		// different values to the ones the user configured.
		//
		// Here we replace "computed" attributes (showing as Unknown) with their actual
		// user-supplied values from "manifest" (if present).
		obj, err = tftypes.Transform(obj, func(ap *tftypes.AttributePath, v tftypes.Value) (tftypes.Value, error) {
			_, isComputed := computedFields[ap.String()]
			if !isComputed {
				return v, nil
			}
			if v.IsKnown() {
				return v, nil
			}
			ppMan, restPath, err := tftypes.WalkAttributePath(plannedStateVal["manifest"], ap)
			if err != nil {
				if len(restPath.Steps()) > 0 {
					// attribute not in manifest
					return v, nil
				}
				return v, ap.NewError(err)
			}
			nv, err := morph.ValueToType(ppMan.(tftypes.Value), v.Type(), tftypes.NewAttributePath())
			if err != nil {
				return v, ap.NewError(err)
			}
			return nv, nil
		})
		if err != nil {
			resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  "Failed to backfill computed values in proposed object",
				Detail:   err.Error(),
			})
			return resp, nil
		}

		minObj := morph.UnknownToNull(obj)
		s.logger.Trace("[ApplyResourceChange][Apply]", "[UnknownToNull]", dump(minObj))

		pu, err := payload.FromTFValue(minObj, tftypes.NewAttributePath())
		if err != nil {
			return resp, err
		}
		s.logger.Trace("[ApplyResourceChange][Apply]", "[payload.FromTFValue]", dump(pu))

		// remove null attributes - the API doesn't appreciate requests that include them
		rqObj := mapRemoveNulls(pu.(map[string]interface{}))

		uo := unstructured.Unstructured{}
		uo.SetUnstructuredContent(rqObj)
		rnamespace := uo.GetNamespace()
		rname := uo.GetName()
		rnn := types.NamespacedName{Namespace: rnamespace, Name: rname}.String()

		gvr, err := GVRFromUnstructured(&uo, m)
		if err != nil {
			return resp, fmt.Errorf("failed to determine resource GVR: %s", err)
		}

		ns, err := IsResourceNamespaced(gvk, m)
		if err != nil {
			resp.Diagnostics = append(resp.Diagnostics,
				&tfprotov5.Diagnostic{
					Severity: tfprotov5.DiagnosticSeverityError,
					Detail:   err.Error(),
					Summary:  fmt.Sprintf("Failed to discover scope of resource '%s'", rnn),
				})
			return resp, nil
		}

		if ns {
			rs = c.Resource(gvr).Namespace(rnamespace)
		} else {
			rs = c.Resource(gvr)
		}

		// Check the resource does not exist if this is a create operation
		if applyPriorState.IsNull() {
			_, err := rs.Get(ctx, rname, metav1.GetOptions{})
			if err == nil {
				resp.Diagnostics = append(resp.Diagnostics,
					&tfprotov5.Diagnostic{
						Severity: tfprotov5.DiagnosticSeverityError,
						Summary:  "Cannot create resource that already exists",
						Detail:   fmt.Sprintf("resource %q already exists", rnn),
					})
				return resp, nil
			} else if !apierrors.IsNotFound(err) {
				resp.Diagnostics = append(resp.Diagnostics,
					&tfprotov5.Diagnostic{
						Severity: tfprotov5.DiagnosticSeverityError,
						Summary:  fmt.Sprintf("Failed to determine if resource %q exists", rnn),
						Detail:   err.Error(),
					})
				return resp, nil
			}
		}

		jsonManifest, err := uo.MarshalJSON()
		if err != nil {
			resp.Diagnostics = append(resp.Diagnostics,
				&tfprotov5.Diagnostic{
					Severity: tfprotov5.DiagnosticSeverityError,
					Detail:   err.Error(),
					Summary:  fmt.Sprintf("Failed to marshall resource '%s' to JSON", rnn),
				})
			return resp, nil
		}

		// Call the Kubernetes API to create the new resource
		result, err := rs.Patch(ctx, rname, types.ApplyPatchType, jsonManifest, metav1.PatchOptions{FieldManager: "Terraform"})
		if err != nil {
			s.logger.Error("[ApplyResourceChange][Apply]", "API error", dump(err), "API response", dump(result))
			if status := apierrors.APIStatus(nil); errors.As(err, &status) {
				resp.Diagnostics = append(resp.Diagnostics, APIStatusErrorToDiagnostics(status.Status())...)
			} else {
				resp.Diagnostics = append(resp.Diagnostics,
					&tfprotov5.Diagnostic{
						Severity: tfprotov5.DiagnosticSeverityError,
						Detail:   err.Error(),
						Summary:  fmt.Sprintf(`PATCH for resource "%s" failed to apply`, rnn),
					})
			}
			return resp, nil
		}

		newResObject, err := payload.ToTFValue(RemoveServerSideFields(result.Object), tsch, tftypes.NewAttributePath())
		if err != nil {
			return resp, err
		}
		s.logger.Trace("[ApplyResourceChange][Apply]", "[payload.ToTFValue]", dump(newResObject))

		wt, err := s.TFTypeFromOpenAPI(ctx, gvk, true)
		if err != nil {
			return resp, fmt.Errorf("failed to determine resource type ID: %s", err)
		}

		wf, ok := plannedStateVal["wait_for"]
		if ok {
			err = s.waitForCompletion(ctx, wf, rs, rname, wt)
			if err != nil {
				return resp, err
			}
		}

		compObj, err := morph.DeepUnknown(tsch, newResObject, tftypes.NewAttributePath())
		if err != nil {
			return resp, err
		}
		plannedStateVal["object"] = morph.UnknownToNull(compObj)

		newStateVal := tftypes.NewValue(applyPlannedState.Type(), plannedStateVal)
		s.logger.Trace("[ApplyResourceChange][Apply]", "new state value", dump(newStateVal))

		newResState, err := tfprotov5.NewDynamicValue(newStateVal.Type(), newStateVal)
		if err != nil {
			return resp, err
		}
		resp.NewState = &newResState
	case applyPlannedState.IsNull():
		// Delete the resource
		priorStateVal := make(map[string]tftypes.Value)
		err = applyPriorState.As(&priorStateVal)
		if err != nil {
			resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  "Failed to extract prior resource state values",
				Detail:   err.Error(),
			})
			return resp, nil
		}
		pco, ok := priorStateVal["object"]
		if !ok {
			resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
				Severity: tfprotov5.DiagnosticSeverityError,
				Summary:  "Failed to find object value in prior resource state",
			})
			return resp, nil
		}

		pu, err := payload.FromTFValue(pco, tftypes.NewAttributePath())
		if err != nil {
			return resp, err
		}

		uo := unstructured.Unstructured{Object: pu.(map[string]interface{})}
		gvr, err := GVRFromUnstructured(&uo, m)
		if err != nil {
			return resp, err
		}

		gvk, err := GVKFromTftypesObject(&pco, m)
		if err != nil {
			return resp, fmt.Errorf("failed to determine resource GVK: %s", err)
		}

		ns, err := IsResourceNamespaced(gvk, m)
		if err != nil {
			return resp, err
		}

		rnamespace := uo.GetNamespace()
		rname := uo.GetName()

		if ns {
			rs = c.Resource(gvr).Namespace(rnamespace)
		} else {
			rs = c.Resource(gvr)
		}
		err = rs.Delete(ctx, rname, metav1.DeleteOptions{})
		if err != nil {
			rn := types.NamespacedName{Namespace: rnamespace, Name: rname}.String()
			resp.Diagnostics = append(resp.Diagnostics,
				&tfprotov5.Diagnostic{
					Severity: tfprotov5.DiagnosticSeverityError,
					Detail:   err.Error(),
					Summary:  fmt.Sprintf("DELETE resource %s failed: %s", rn, err),
				})
			return resp, nil
		}

		resp.NewState = req.PlannedState
	}
	// force a refresh of the OpenAPI foundry on next use
	// we do this to capture any potentially new resource type that might have been added
	s.OAPIFoundry = nil // this needs to be optimized to refresh only when CRDs are applied (or maybe other schema altering resources too?)

	return resp, nil
}
