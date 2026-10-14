"""S05-only immutable admission policy and executed export/composition evidence.

Approved export contracts: docs/contracts/ai-integration/export-closure.md.
Independent tables below are fixed authority; candidate fields cannot redefine them.
The S04 source identities and independent evidence guards remain separate.
"""
from __future__ import annotations
import collections
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib

try:
    from .upstream import resolve_upstream, upstream_environment
except ImportError:
    from upstream import resolve_upstream, upstream_environment

ROOT = Path(__file__).resolve().parents[1]
PIN = "853a80d26c90a14c1886f0ebb8ffaae133ca2185"
SCOPE_DIGEST = "c9fcb2d45dcde483d82a475fbb28f92868162199fee18b78cec0925fa17aaa3e"
CATALOG = "conformance/ai-domain/s05-export-closure/evidence.json"
CATALOG_DIGEST = "dc6f1d6f8f06114025e0cf94ad7769e9d522ddaa286d04cda4ed8b5721a8899c"

TEST_POLICY = {'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@': {'features': [],
                                                                                                     'kind': 'test',
                                                                                                     'name': 'auth_foundation_complete_pinned_source_and_hashes_remain_exact',
                                                                                                     'package': 'pi-ai-providers',
                                                                                                     'sha256': 'faba44a45c5d06eb8dea05e26cd610b91c49070f7a24cf957ccb0699945e20ac',
                                                                                                     'target': 'auth_foundation'},
 'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@': {'features': [],
                                                                                            'kind': 'test',
                                                                                            'name': 'auth_foundation_context_and_selected_legacy_discovery',
                                                                                            'package': 'pi-ai-providers',
                                                                                            'sha256': 'faba44a45c5d06eb8dea05e26cd610b91c49070f7a24cf957ccb0699945e20ac',
                                                                                            'target': 'auth_foundation'},
 'pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@': {'features': [],
                                                                                                 'kind': 'test',
                                                                                                 'name': 'auth_foundation_key_precedence_truthiness_and_cancellation',
                                                                                                 'package': 'pi-ai-providers',
                                                                                                 'sha256': 'faba44a45c5d06eb8dea05e26cd610b91c49070f7a24cf957ccb0699945e20ac',
                                                                                                 'target': 'auth_foundation'},
 'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@': {'features': [],
                                                                                                        'kind': 'test',
                                                                                                        'name': 'auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load',
                                                                                                        'package': 'pi-ai-providers',
                                                                                                        'sha256': 'faba44a45c5d06eb8dea05e26cd610b91c49070f7a24cf957ccb0699945e20ac',
                                                                                                        'target': 'auth_foundation'},
 'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@': {'features': [],
                                                                                                     'kind': 'test',
                                                                                                     'name': 'auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop',
                                                                                                     'package': 'pi-ai-providers',
                                                                                                     'sha256': 'faba44a45c5d06eb8dea05e26cd610b91c49070f7a24cf957ccb0699945e20ac',
                                                                                                     'target': 'auth_foundation'},
 'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@': {'features': [],
                                                                                                                   'kind': 'test',
                                                                                                                   'name': 'auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop',
                                                                                                                   'package': 'pi-ai-providers',
                                                                                                                   'sha256': 'faba44a45c5d06eb8dea05e26cd610b91c49070f7a24cf957ccb0699945e20ac',
                                                                                                                   'target': 'auth_foundation'},
 'pi-ai-providers/catalog#captured_anthropic_matches_complete_source@catalog-anthropic': {'features': ['catalog-anthropic'],
                                                                                          'kind': 'test',
                                                                                          'name': 'captured_anthropic_matches_complete_source',
                                                                                          'package': 'pi-ai-providers',
                                                                                          'sha256': 'b1de016ee88cdf8e8c5290ea05deb5c0d62e88ec35f0b29c1866c1d198a78031',
                                                                                          'target': 'catalog'},
 'pi-ai-providers/catalog#captured_codex_matches_complete_source@catalog-openai-codex': {'features': ['catalog-openai-codex'],
                                                                                         'kind': 'test',
                                                                                         'name': 'captured_codex_matches_complete_source',
                                                                                         'package': 'pi-ai-providers',
                                                                                         'sha256': 'b1de016ee88cdf8e8c5290ea05deb5c0d62e88ec35f0b29c1866c1d198a78031',
                                                                                         'target': 'catalog'},
 'pi-ai-providers/catalog#captured_deepseek_matches_complete_source@catalog-deepseek': {'features': ['catalog-deepseek'],
                                                                                        'kind': 'test',
                                                                                        'name': 'captured_deepseek_matches_complete_source',
                                                                                        'package': 'pi-ai-providers',
                                                                                        'sha256': 'b1de016ee88cdf8e8c5290ea05deb5c0d62e88ec35f0b29c1866c1d198a78031',
                                                                                        'target': 'catalog'},
 'pi-ai-providers/catalog#captured_google_matches_complete_source@catalog-google': {'features': ['catalog-google'],
                                                                                    'kind': 'test',
                                                                                    'name': 'captured_google_matches_complete_source',
                                                                                    'package': 'pi-ai-providers',
                                                                                    'sha256': 'b1de016ee88cdf8e8c5290ea05deb5c0d62e88ec35f0b29c1866c1d198a78031',
                                                                                    'target': 'catalog'},
 'pi-ai-providers/catalog#captured_openai_matches_complete_source@catalog-openai': {'features': ['catalog-openai'],
                                                                                    'kind': 'test',
                                                                                    'name': 'captured_openai_matches_complete_source',
                                                                                    'package': 'pi-ai-providers',
                                                                                    'sha256': 'b1de016ee88cdf8e8c5290ea05deb5c0d62e88ec35f0b29c1866c1d198a78031',
                                                                                    'target': 'catalog'},
 'pi-ai-providers/catalog#captured_openrouter_matches_complete_source@catalog-openrouter': {'features': ['catalog-openrouter'],
                                                                                            'kind': 'test',
                                                                                            'name': 'captured_openrouter_matches_complete_source',
                                                                                            'package': 'pi-ai-providers',
                                                                                            'sha256': 'b1de016ee88cdf8e8c5290ea05deb5c0d62e88ec35f0b29c1866c1d198a78031',
                                                                                            'target': 'catalog'},
 'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@': {'features': [],
                                                                                                               'kind': 'test',
                                                                                                               'name': 'all_provider_refresh_admits_every_future_and_settles_failures_best_effort',
                                                                                                               'package': 'pi-ai-providers',
                                                                                                               'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                               'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@': {'features': [],
                                                                                                              'kind': 'test',
                                                                                                              'name': 'cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters',
                                                                                                              'package': 'pi-ai-providers',
                                                                                                              'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                              'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@': {'features': [],
                                                                                                                     'kind': 'test',
                                                                                                                     'name': 'complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge',
                                                                                                                     'package': 'pi-ai-providers',
                                                                                                                     'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                     'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@': {'features': [],
                                                                                                                        'kind': 'test',
                                                                                                                        'name': 'complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure',
                                                                                                                        'package': 'pi-ai-providers',
                                                                                                                        'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                        'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@': {'features': [],
                                                                                                                            'kind': 'test',
                                                                                                                            'name': 'complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry',
                                                                                                                            'package': 'pi-ai-providers',
                                                                                                                            'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                            'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@': {'features': [],
                                                                                                                               'kind': 'test',
                                                                                                                               'name': 'complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing',
                                                                                                                               'package': 'pi-ai-providers',
                                                                                                                               'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                               'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@': {'features': [],
                                                                                                                'kind': 'test',
                                                                                                                'name': 'configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter',
                                                                                                                'package': 'pi-ai-providers',
                                                                                                                'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@': {'features': [],
                                                                                                                      'kind': 'test',
                                                                                                                      'name': 'explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects',
                                                                                                                      'package': 'pi-ai-providers',
                                                                                                                      'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                      'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@': {'features': [],
                                                                                                                       'kind': 'test',
                                                                                                                       'name': 'shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters',
                                                                                                                       'package': 'pi-ai-providers',
                                                                                                                       'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                       'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@': {'features': [],
                                                                                                                       'kind': 'test',
                                                                                                                       'name': 'source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned',
                                                                                                                       'package': 'pi-ai-providers',
                                                                                                                       'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                       'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@': {'features': [],
                                                                                                                   'kind': 'test',
                                                                                                                   'name': 'source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries',
                                                                                                                   'package': 'pi-ai-providers',
                                                                                                                   'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                   'target': 'images_runtime'},
 'pi-ai-providers/images_runtime#wire_integration::complete_runtime_auth_dispatch_to_accepted_wire_preserves_endpoint_and_callback_cancel_success@wire-openrouter-images': {'features': ['wire-openrouter-images'],
                                                                                                                                                                            'kind': 'test',
                                                                                                                                                                            'name': 'wire_integration::complete_runtime_auth_dispatch_to_accepted_wire_preserves_endpoint_and_callback_cancel_success',
                                                                                                                                                                            'package': 'pi-ai-providers',
                                                                                                                                                                            'sha256': 'f2f9383c0dcf65303854790fb2edbfaad2579a03f22333c27224e62bfc02c6e8',
                                                                                                                                                                            'target': 'images_runtime'},
 'pi-ai-providers/oauth_cli_host#oauth_cli_actual_selected_four_login_paths_and_export@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                       'oauth-anthropic',
                                                                                                                                                                       'oauth-openai-codex',
                                                                                                                                                                       'oauth-openrouter'],
                                                                                                                                                          'kind': 'test',
                                                                                                                                                          'name': 'oauth_cli_actual_selected_four_login_paths_and_export',
                                                                                                                                                          'package': 'pi-ai-providers',
                                                                                                                                                          'sha256': '3205c89814f177a6e5feeb448ee77f2ff07f96877234d2323012b87c10bfb1de',
                                                                                                                                                          'target': 'oauth_cli_host'},
 'pi-ai-providers/oauth_cli_host#oauth_cli_browser_success_exits_while_physical_stdin_remains_blocked@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                      'oauth-anthropic',
                                                                                                                                                                                      'oauth-openai-codex',
                                                                                                                                                                                      'oauth-openrouter'],
                                                                                                                                                                         'kind': 'test',
                                                                                                                                                                         'name': 'oauth_cli_browser_success_exits_while_physical_stdin_remains_blocked',
                                                                                                                                                                         'package': 'pi-ai-providers',
                                                                                                                                                                         'sha256': '3205c89814f177a6e5feeb448ee77f2ff07f96877234d2323012b87c10bfb1de',
                                                                                                                                                                         'target': 'oauth_cli_host'},
 'pi-ai-providers/oauth_cli_host#oauth_cli_included_source_hashes_bind_host_proof@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                  'oauth-anthropic',
                                                                                                                                                                  'oauth-openai-codex',
                                                                                                                                                                  'oauth-openrouter'],
                                                                                                                                                     'kind': 'test',
                                                                                                                                                     'name': 'oauth_cli_included_source_hashes_bind_host_proof',
                                                                                                                                                     'package': 'pi-ai-providers',
                                                                                                                                                     'sha256': '3205c89814f177a6e5feeb448ee77f2ff07f96877234d2323012b87c10bfb1de',
                                                                                                                                                     'target': 'oauth_cli_host'},
 'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_callback_reply_and_cleanup_stages@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                             'oauth-anthropic',
                                                                                                                                                                                             'oauth-openai-codex',
                                                                                                                                                                                             'oauth-openrouter'],
                                                                                                                                                                                'kind': 'test',
                                                                                                                                                                                'name': 'native_composition::oauth_selected_native_callback_reply_and_cleanup_stages',
                                                                                                                                                                                'package': 'pi-ai-providers',
                                                                                                                                                                                'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                                'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_cancellation_releases_listener_and_prompt@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                                     'oauth-anthropic',
                                                                                                                                                                                                     'oauth-openai-codex',
                                                                                                                                                                                                     'oauth-openrouter'],
                                                                                                                                                                                        'kind': 'test',
                                                                                                                                                                                        'name': 'native_composition::oauth_selected_native_cancellation_releases_listener_and_prompt',
                                                                                                                                                                                        'package': 'pi-ai-providers',
                                                                                                                                                                                        'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                                        'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_codex_bind_fallback_and_anthropic_failure@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                                     'oauth-anthropic',
                                                                                                                                                                                                     'oauth-openai-codex',
                                                                                                                                                                                                     'oauth-openrouter'],
                                                                                                                                                                                        'kind': 'test',
                                                                                                                                                                                        'name': 'native_composition::oauth_selected_native_codex_bind_fallback_and_anthropic_failure',
                                                                                                                                                                                        'package': 'pi-ai-providers',
                                                                                                                                                                                        'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                                        'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_stalled_primary_ack_retains_cancellation_and_timeout_owners@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                                                       'oauth-anthropic',
                                                                                                                                                                                                                       'oauth-openai-codex',
                                                                                                                                                                                                                       'oauth-openrouter'],
                                                                                                                                                                                                          'kind': 'test',
                                                                                                                                                                                                          'name': 'native_composition::oauth_selected_native_stalled_primary_ack_retains_cancellation_and_timeout_owners',
                                                                                                                                                                                                          'package': 'pi-ai-providers',
                                                                                                                                                                                                          'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                                                          'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#oauth_openrouter_callback_timeout_preserves_fixed_page_detail@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                               'oauth-anthropic',
                                                                                                                                                                               'oauth-openai-codex',
                                                                                                                                                                               'oauth-openrouter'],
                                                                                                                                                                  'kind': 'test',
                                                                                                                                                                  'name': 'oauth_openrouter_callback_timeout_preserves_fixed_page_detail',
                                                                                                                                                                  'package': 'pi-ai-providers',
                                                                                                                                                                  'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                  'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#oauth_openrouter_successful_body_read_failure_is_json_admission@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                 'oauth-anthropic',
                                                                                                                                                                                 'oauth-openai-codex',
                                                                                                                                                                                 'oauth-openrouter'],
                                                                                                                                                                    'kind': 'test',
                                                                                                                                                                    'name': 'oauth_openrouter_successful_body_read_failure_is_json_admission',
                                                                                                                                                                    'package': 'pi-ai-providers',
                                                                                                                                                                    'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                    'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#oauth_selected_complete_source_trace_parity@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                             'oauth-anthropic',
                                                                                                                                                             'oauth-openai-codex',
                                                                                                                                                             'oauth-openrouter'],
                                                                                                                                                'kind': 'test',
                                                                                                                                                'name': 'oauth_selected_complete_source_trace_parity',
                                                                                                                                                'package': 'pi-ai-providers',
                                                                                                                                                'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#oauth_selected_full_pinned_source_reexecutes_with_exact_hashes@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                'oauth-anthropic',
                                                                                                                                                                                'oauth-openai-codex',
                                                                                                                                                                                'oauth-openrouter'],
                                                                                                                                                                   'kind': 'test',
                                                                                                                                                                   'name': 'oauth_selected_full_pinned_source_reexecutes_with_exact_hashes',
                                                                                                                                                                   'package': 'pi-ai-providers',
                                                                                                                                                                   'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                   'target': 'oauth_selected'},
 'pi-ai-providers/oauth_selected#oauth_selected_source_credential_values_match_without_logging_tokens@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter': {'features': ['auth-native',
                                                                                                                                                                                      'oauth-anthropic',
                                                                                                                                                                                      'oauth-openai-codex',
                                                                                                                                                                                      'oauth-openrouter'],
                                                                                                                                                                         'kind': 'test',
                                                                                                                                                                         'name': 'oauth_selected_source_credential_values_match_without_logging_tokens',
                                                                                                                                                                         'package': 'pi-ai-providers',
                                                                                                                                                                         'sha256': '0634c5da5e2e06285bf6054083a62ce554896f5a34302232b804eb386467f0ed',
                                                                                                                                                                         'target': 'oauth_selected'},
 'pi-ai-providers/openrouter_images#callback_cancellation_does_not_add_a_success_veto_and_errors_keep_partial_output@wire-openrouter-images': {'features': ['wire-openrouter-images'],
                                                                                                                                               'kind': 'test',
                                                                                                                                               'name': 'callback_cancellation_does_not_add_a_success_veto_and_errors_keep_partial_output',
                                                                                                                                               'package': 'pi-ai-providers',
                                                                                                                                               'sha256': 'e63bc3447d47e97479186c32e3ee6bee999ac936dfeb52fae444ade6c5e51ed0',
                                                                                                                                               'target': 'openrouter_images'},
 'pi-ai-providers/openrouter_images#complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages@wire-openrouter-images': {'features': ['wire-openrouter-images'],
                                                                                                                                                    'kind': 'test',
                                                                                                                                                    'name': 'complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages',
                                                                                                                                                    'package': 'pi-ai-providers',
                                                                                                                                                    'sha256': 'e63bc3447d47e97479186c32e3ee6bee999ac936dfeb52fae444ade6c5e51ed0',
                                                                                                                                                    'target': 'openrouter_images'},
 'pi-ai-providers/openrouter_images#inherited_numeric_admission_is_checked_without_changing_usage_math@wire-openrouter-images': {'features': ['wire-openrouter-images'],
                                                                                                                                 'kind': 'test',
                                                                                                                                 'name': 'inherited_numeric_admission_is_checked_without_changing_usage_math',
                                                                                                                                 'package': 'pi-ai-providers',
                                                                                                                                 'sha256': 'e63bc3447d47e97479186c32e3ee6bee999ac936dfeb52fae444ade6c5e51ed0',
                                                                                                                                 'target': 'openrouter_images'},
 'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers': {'features': ['all-images',
                                                                                                                                                                'all-oauth',
                                                                                                                                                                'all-providers'],
                                                                                                                                                   'kind': 'test',
                                                                                                                                                   'name': 'awaited_builtin_registration_and_each_selected_factory_reaches_its_wire',
                                                                                                                                                   'package': 'pi-ai-providers',
                                                                                                                                                   'sha256': 'b1c3a621e98ae16afec403f2364be3c190f937afed72a97e625e78ce852c0771',
                                                                                                                                                   'target': 'selected_factories'},
 'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers': {'features': ['all-images',
                                                                                                                                                                  'all-oauth',
                                                                                                                                                                  'all-providers'],
                                                                                                                                                     'kind': 'test',
                                                                                                                                                     'name': 'builtin_child_cancellation_retains_usage17_after_real_completions_handoff',
                                                                                                                                                     'package': 'pi-ai-providers',
                                                                                                                                                     'sha256': 'b1c3a621e98ae16afec403f2364be3c190f937afed72a97e625e78ce852c0771',
                                                                                                                                                     'target': 'selected_factories'},
 'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers': {'features': ['all-images',
                                                                                                                                                       'all-oauth',
                                                                                                                                                       'all-providers'],
                                                                                                                                          'kind': 'test',
                                                                                                                                          'name': 'facade_awaits_registration_failure_before_returning_a_registry',
                                                                                                                                          'package': 'pi-ai-providers',
                                                                                                                                          'sha256': 'b1c3a621e98ae16afec403f2364be3c190f937afed72a97e625e78ce852c0771',
                                                                                                                                          'target': 'selected_factories'},
 'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers': {'features': ['all-images',
                                                                                                                                                            'all-oauth',
                                                                                                                                                            'all-providers'],
                                                                                                                                               'kind': 'test',
                                                                                                                                               'name': 'selected_lazy_capabilities_and_optional_oauth_loads_remain_separate',
                                                                                                                                               'package': 'pi-ai-providers',
                                                                                                                                               'sha256': 'b1c3a621e98ae16afec403f2364be3c190f937afed72a97e625e78ce852c0771',
                                                                                                                                               'target': 'selected_factories'},
 'pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers': {'features': ['all-images',
                                                                                                                                                              'all-oauth',
                                                                                                                                                              'all-providers'],
                                                                                                                                                 'kind': 'test',
                                                                                                                                                 'name': 'source_metadata_catalog_order_static_capabilities_and_passive_modules',
                                                                                                                                                 'package': 'pi-ai-providers',
                                                                                                                                                 'sha256': 'b1c3a621e98ae16afec403f2364be3c190f937afed72a97e625e78ce852c0771',
                                                                                                                                                 'target': 'selected_factories'},
 'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers': {'features': ['all-images',
                                                                                                                                                'all-providers'],
                                                                                                                                   'kind': 'test',
                                                                                                                                   'name': 'actual_linked_facade_assembly_and_partial_profiles_execute',
                                                                                                                                   'package': 'pi-ai-providers',
                                                                                                                                   'sha256': '4e3c1a1d4c752d37a77487671d0127e0c183b712d6549e87d4d758aca7921d85',
                                                                                                                                   'target': 'selected_factories_source'},
 'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@': {'features': [],
                                                                                                              'kind': 'test',
                                                                                                              'name': 'complete_pinned_selected_factory_source_and_capture_are_fresh',
                                                                                                              'package': 'pi-ai-providers',
                                                                                                              'sha256': '4e3c1a1d4c752d37a77487671d0127e0c183b712d6549e87d4d758aca7921d85',
                                                                                                              'target': 'selected_factories_source'},
 'pi-ai/catalog_binaries#actual_binaries_help_default_check_and_exact_deterministic_family_outputs@dev-catalog-tools': {'features': ['dev-catalog-tools'],
                                                                                                                        'kind': 'test',
                                                                                                                        'name': 'actual_binaries_help_default_check_and_exact_deterministic_family_outputs',
                                                                                                                        'package': 'pi-ai',
                                                                                                                        'sha256': '064733387a0dcaccc55424f8f654bf637afdac5ff6b93feebbb9b16476cfeafb',
                                                                                                                        'target': 'catalog_binaries'},
 'pi-ai/catalog_binaries#actual_binaries_reject_changed_capture_source_and_canonical_output_after_generation@dev-catalog-tools': {'features': ['dev-catalog-tools'],
                                                                                                                                  'kind': 'test',
                                                                                                                                  'name': 'actual_binaries_reject_changed_capture_source_and_canonical_output_after_generation',
                                                                                                                                  'package': 'pi-ai',
                                                                                                                                  'sha256': '064733387a0dcaccc55424f8f654bf637afdac5ff6b93feebbb9b16476cfeafb',
                                                                                                                                  'target': 'catalog_binaries'},
 'pi-ai/catalog_binaries#actual_binaries_reject_unknown_arguments_and_missing_workspace_without_output@dev-catalog-tools': {'features': ['dev-catalog-tools'],
                                                                                                                            'kind': 'test',
                                                                                                                            'name': 'actual_binaries_reject_unknown_arguments_and_missing_workspace_without_output',
                                                                                                                            'package': 'pi-ai',
                                                                                                                            'sha256': '064733387a0dcaccc55424f8f654bf637afdac5ff6b93feebbb9b16476cfeafb',
                                                                                                                            'target': 'catalog_binaries'},
 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat': {'features': ['compat'],
                                                                                             'kind': 'test',
                                                                                             'name': 'actual_linked_facade_executes_nine_non_native_contract_tests',
                                                                                             'package': 'pi-ai',
                                                                                             'sha256': '4a6ba5ccff95885b92b174b41e36fef4a0d3cbe5191cb69fa9ae815bde1591ab',
                                                                                             'target': 'compat_facade'},
 'pi-ai/compat_facade#actual_linked_facade_executes_physical_codex_snapshot_and_drop_proof@compat,native': {'features': ['compat',
                                                                                                                         'native'],
                                                                                                            'kind': 'test',
                                                                                                            'name': 'actual_linked_facade_executes_physical_codex_snapshot_and_drop_proof',
                                                                                                            'package': 'pi-ai',
                                                                                                            'sha256': '4a6ba5ccff95885b92b174b41e36fef4a0d3cbe5191cb69fa9ae815bde1591ab',
                                                                                                            'target': 'compat_facade'},
 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat': {'features': ['compat'],
                                                                                            'kind': 'test',
                                                                                            'name': 'actual_root_compat_and_oauth_projection_routes_are_callable',
                                                                                            'package': 'pi-ai',
                                                                                            'sha256': '4a6ba5ccff95885b92b174b41e36fef4a0d3cbe5191cb69fa9ae815bde1591ab',
                                                                                            'target': 'compat_facade'},
 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat': {'features': ['compat'],
                                                                                            'kind': 'test',
                                                                                            'name': 'complete_source_facade_and_discriminating_registry_controls',
                                                                                            'package': 'pi-ai',
                                                                                            'sha256': '4a6ba5ccff95885b92b174b41e36fef4a0d3cbe5191cb69fa9ae815bde1591ab',
                                                                                            'target': 'compat_facade'},
 'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat': {'features': ['compat'],
                                                                                                  'kind': 'test',
                                                                                                  'name': 'compat_oauth_complete_pinned_source_declarations_and_exports',
                                                                                                  'package': 'pi-ai',
                                                                                                  'sha256': '6061335a7d8adeec1d582bbbbfe617772374ce23ad1830fb481cfaf064927fc0',
                                                                                                  'target': 'compat_oauth_types'},
 'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat': {'features': ['compat'],
                                                                                                          'kind': 'test',
                                                                                                          'name': 'compat_oauth_credentials_reexport_is_the_existing_nominal_core_value',
                                                                                                          'package': 'pi-ai',
                                                                                                          'sha256': '6061335a7d8adeec1d582bbbbfe617772374ce23ad1830fb481cfaf064927fc0',
                                                                                                          'target': 'compat_oauth_types'},
 'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat': {'features': ['compat'],
                                                                                                     'kind': 'test',
                                                                                                     'name': 'compat_oauth_optional_presence_is_observable_without_invocation',
                                                                                                     'package': 'pi-ai',
                                                                                                     'sha256': '6061335a7d8adeec1d582bbbbfe617772374ce23ad1830fb481cfaf064927fc0',
                                                                                                     'target': 'compat_oauth_types'},
 'pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat': {'features': ['compat'],
                                                                                                                  'kind': 'test',
                                                                                                                  'name': 'compat_oauth_required_trait_objects_preserve_values_order_and_select_absence',
                                                                                                                  'package': 'pi-ai',
                                                                                                                  'sha256': '6061335a7d8adeec1d582bbbbfe617772374ce23ad1830fb481cfaf064927fc0',
                                                                                                                  'target': 'compat_oauth_types'},
 'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat': {'features': ['compat'],
                                                                                                   'kind': 'test',
                                                                                                   'name': 'compat_oauth_sync_and_async_errors_retain_the_callback_source',
                                                                                                   'package': 'pi-ai',
                                                                                                   'sha256': '6061335a7d8adeec1d582bbbbfe617772374ce23ad1830fb481cfaf064927fc0',
                                                                                                   'target': 'compat_oauth_types'},
 'pi-ai/oauth_cli#oauth_cli_32_source_command_observations_and_explicit_normalizations@dev-oauth-cli': {'features': ['dev-oauth-cli'],
                                                                                                        'kind': 'test',
                                                                                                        'name': 'oauth_cli_32_source_command_observations_and_explicit_normalizations',
                                                                                                        'package': 'pi-ai',
                                                                                                        'sha256': '2d1b1408a9c618341e00a2b2aaf53019abcd7965bac161ecfd22e064f3b02c54',
                                                                                                        'target': 'oauth_cli'},
 'pi-ai/oauth_cli#oauth_cli_auth_file_admission_and_preservation_controls@dev-oauth-cli': {'features': ['dev-oauth-cli'],
                                                                                           'kind': 'test',
                                                                                           'name': 'oauth_cli_auth_file_admission_and_preservation_controls',
                                                                                           'package': 'pi-ai',
                                                                                           'sha256': '2d1b1408a9c618341e00a2b2aaf53019abcd7965bac161ecfd22e064f3b02c54',
                                                                                           'target': 'oauth_cli'},
 'pi-ai/oauth_cli#oauth_cli_complete_pinned_source_reexecution@dev-oauth-cli': {'features': ['dev-oauth-cli'],
                                                                                'kind': 'test',
                                                                                'name': 'oauth_cli_complete_pinned_source_reexecution',
                                                                                'package': 'pi-ai',
                                                                                'sha256': '2d1b1408a9c618341e00a2b2aaf53019abcd7965bac161ecfd22e064f3b02c54',
                                                                                'target': 'oauth_cli'},
 'pi-ai/oauth_cli#oauth_cli_decimal_prefix_and_prompt_variance@dev-oauth-cli': {'features': ['dev-oauth-cli'],
                                                                                'kind': 'test',
                                                                                'name': 'oauth_cli_decimal_prefix_and_prompt_variance',
                                                                                'package': 'pi-ai',
                                                                                'sha256': '2d1b1408a9c618341e00a2b2aaf53019abcd7965bac161ecfd22e064f3b02c54',
                                                                                'target': 'oauth_cli'},
 'pi-ai/oauth_cli#oauth_cli_reader_cancellation_identity_close_empty_and_eof@dev-oauth-cli': {'features': ['dev-oauth-cli'],
                                                                                              'kind': 'test',
                                                                                              'name': 'oauth_cli_reader_cancellation_identity_close_empty_and_eof',
                                                                                              'package': 'pi-ai',
                                                                                              'sha256': '2d1b1408a9c618341e00a2b2aaf53019abcd7965bac161ecfd22e064f3b02c54',
                                                                                              'target': 'oauth_cli'},
 'pi-ai/oauth_cli#oauth_cli_real_binary_no_network_help_errors_pipes_and_eof@dev-oauth-cli': {'features': ['dev-oauth-cli'],
                                                                                              'kind': 'test',
                                                                                              'name': 'oauth_cli_real_binary_no_network_help_errors_pipes_and_eof',
                                                                                              'package': 'pi-ai',
                                                                                              'sha256': '2d1b1408a9c618341e00a2b2aaf53019abcd7965bac161ecfd22e064f3b02c54',
                                                                                              'target': 'oauth_cli'},
 'pi-ai/retry#retry_cancellation_belongs_to_sleep@': {'features': [],
                                                      'kind': 'test',
                                                      'name': 'retry_cancellation_belongs_to_sleep',
                                                      'package': 'pi-ai',
                                                      'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                      'target': 'retry'},
 'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@': {'features': [],
                                                                     'kind': 'test',
                                                                     'name': 'retry_complete_pinned_source_and_timer_fingerprint',
                                                                     'package': 'pi-ai',
                                                                     'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                                     'target': 'retry'},
 'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@': {'features': [],
                                                                          'kind': 'test',
                                                                          'name': 'retry_extreme_numeric_observations_and_clock_projection',
                                                                          'package': 'pi-ai',
                                                                          'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                                          'target': 'retry'},
 'pi-ai/retry#retry_loop_aborted_after_scheduled@': {'features': [],
                                                     'kind': 'test',
                                                     'name': 'retry_loop_aborted_after_scheduled',
                                                     'package': 'pi-ai',
                                                     'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                     'target': 'retry'},
 'pi-ai/retry#retry_loop_cancel_during_sleep@': {'features': [],
                                                 'kind': 'test',
                                                 'name': 'retry_loop_cancel_during_sleep',
                                                 'package': 'pi-ai',
                                                 'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                 'target': 'retry'},
 'pi-ai/retry#retry_loop_exhaustion_call_count@': {'features': [],
                                                   'kind': 'test',
                                                   'name': 'retry_loop_exhaustion_call_count',
                                                   'package': 'pi-ai',
                                                   'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                   'target': 'retry'},
 'pi-ai/retry#retry_loop_exponential_delays@': {'features': [],
                                                'kind': 'test',
                                                'name': 'retry_loop_exponential_delays',
                                                'package': 'pi-ai',
                                                'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                'target': 'retry'},
 'pi-ai/retry#retry_loop_finish_only_after_retry@': {'features': [],
                                                     'kind': 'test',
                                                     'name': 'retry_loop_finish_only_after_retry',
                                                     'package': 'pi-ai',
                                                     'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                     'target': 'retry'},
 'pi-ai/retry#retry_loop_finish_outcomes@': {'features': [],
                                             'kind': 'test',
                                             'name': 'retry_loop_finish_outcomes',
                                             'package': 'pi-ai',
                                             'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                             'target': 'retry'},
 'pi-ai/retry#retry_loop_immediate_aborted@': {'features': [],
                                               'kind': 'test',
                                               'name': 'retry_loop_immediate_aborted',
                                               'package': 'pi-ai',
                                               'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                               'target': 'retry'},
 'pi-ai/retry#retry_loop_immediate_nonretryable@': {'features': [],
                                                    'kind': 'test',
                                                    'name': 'retry_loop_immediate_nonretryable',
                                                    'package': 'pi-ai',
                                                    'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                    'target': 'retry'},
 'pi-ai/retry#retry_loop_immediate_success@': {'features': [],
                                               'kind': 'test',
                                               'name': 'retry_loop_immediate_success',
                                               'package': 'pi-ai',
                                               'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                               'target': 'retry'},
 'pi-ai/retry#retry_loop_later_success@': {'features': [],
                                           'kind': 'test',
                                           'name': 'retry_loop_later_success',
                                           'package': 'pi-ai',
                                           'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                           'target': 'retry'},
 'pi-ai/retry#retry_loop_observer_order@': {'features': [],
                                            'kind': 'test',
                                            'name': 'retry_loop_observer_order',
                                            'package': 'pi-ai',
                                            'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                            'target': 'retry'},
 'pi-ai/retry#retry_loop_policy_absent@': {'features': [],
                                           'kind': 'test',
                                           'name': 'retry_loop_policy_absent',
                                           'package': 'pi-ai',
                                           'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                           'target': 'retry'},
 'pi-ai/retry#retry_loop_policy_disabled@': {'features': [],
                                             'kind': 'test',
                                             'name': 'retry_loop_policy_disabled',
                                             'package': 'pi-ai',
                                             'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                             'target': 'retry'},
 'pi-ai/retry#retry_loop_zero_retries@': {'features': [],
                                          'kind': 'test',
                                          'name': 'retry_loop_zero_retries',
                                          'package': 'pi-ai',
                                          'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                          'target': 'retry'},
 'pi-ai/retry#retry_rejected_operations_preserve_source_stage@': {'features': [],
                                                                  'kind': 'test',
                                                                  'name': 'retry_rejected_operations_preserve_source_stage',
                                                                  'package': 'pi-ai',
                                                                  'sha256': '17ca6d346dcb400fb4d9d2bd5ab635023292760fbfe716795b507df94676718d',
                                                                  'target': 'retry'},
 'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat': {'features': ['compat'],
                                                                                                   'kind': 'test',
                                                                                                   'name': 's05_selected_legacy_env_complete_source_and_facade_results',
                                                                                                   'package': 'pi-ai',
                                                                                                   'sha256': 'fea4d2ec565acf3a70a296f9a8f63aa0af71973ff73dab3ffaa42a3c7b25dfd4',
                                                                                                   'target': 's05_export_legacy_env'}}

SYMBOL_POLICY = {'packages/ai/src/api/openrouter-images.lazy.ts:openrouterImagesApi': {'occurrences': [{'key': './api/openrouter-images.lazy:openrouterImagesApi',
                                                                                        'routeFeatures': [],
                                                                                        'routePackage': 'pi-ai-providers',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      '_: '
                                                                                                      'fn(Arc<dyn '
                                                                                                      'img::ImagesApiLoader>)->img::PerCallImagesApi '
                                                                                                      '= '
                                                                                                      '$target;}',
                                                                                        'routeTarget': 'pi_ai_providers::images::PerCallImagesApi::new',
                                                                                        'sources': ['images-runtime'],
                                                                                        'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                                  'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                                  'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                                  'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                                  'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                                  'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                                  'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                                  'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                                  'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                                  'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                                  'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                                       'owningFeatures': [],
                                                                       'owningPackage': 'pi-ai-providers',
                                                                       'owningProbe': 'fn probe(){let _: '
                                                                                      'fn(Arc<dyn '
                                                                                      'img::ImagesApiLoader>)->img::PerCallImagesApi '
                                                                                      '= $target;}',
                                                                       'owningTarget': 'pi_ai_providers::images::PerCallImagesApi::new',
                                                                       'sourceKind': 'value',
                                                                       'sources': ['images-runtime'],
                                                                       'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                 'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                 'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                 'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                 'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                 'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                 'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                 'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                 'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                 'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                 'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/api/openrouter-images.ts:generateImages': {'occurrences': [{'key': './api/openrouter-images:generateImages',
                                                                              'routeFeatures': ['wire-openrouter-images'],
                                                                              'routePackage': 'pi-ai-providers',
                                                                              'routeProbe': 'fn '
                                                                                            "output<'a,F:Future<Output=AssistantImages>+Send+'a>(_:F){} "
                                                                                            'fn '
                                                                                            "probe<'a>(r:ImageGenerationRequest,s:&'a "
                                                                                            'img::ImageAdapterServices){output($target(r,s));} '
                                                                                            'fn '
                                                                                            'adapter(a:&img::openrouter::OpenRouterImagesApi){let '
                                                                                            '_: &dyn '
                                                                                            'ImagesApiAdapter=a;}',
                                                                              'routeTarget': 'pi_ai_providers::images::openrouter::generate_images',
                                                                              'sources': ['image-wire'],
                                                                              'tests': ['pi-ai-providers/openrouter_images#complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages@wire-openrouter-images',
                                                                                        'pi-ai-providers/openrouter_images#callback_cancellation_does_not_add_a_success_veto_and_errors_keep_partial_output@wire-openrouter-images',
                                                                                        'pi-ai-providers/openrouter_images#inherited_numeric_admission_is_checked_without_changing_usage_math@wire-openrouter-images',
                                                                                        'pi-ai-providers/images_runtime#wire_integration::complete_runtime_auth_dispatch_to_accepted_wire_preserves_endpoint_and_callback_cancel_success@wire-openrouter-images']}],
                                                             'owningFeatures': ['wire-openrouter-images'],
                                                             'owningPackage': 'pi-ai-providers',
                                                             'owningProbe': 'fn '
                                                                            "output<'a,F:Future<Output=AssistantImages>+Send+'a>(_:F){} "
                                                                            'fn '
                                                                            "probe<'a>(r:ImageGenerationRequest,s:&'a "
                                                                            'img::ImageAdapterServices){output($target(r,s));} '
                                                                            'fn '
                                                                            'adapter(a:&img::openrouter::OpenRouterImagesApi){let '
                                                                            '_: &dyn ImagesApiAdapter=a;}',
                                                             'owningTarget': 'pi_ai_providers::images::openrouter::generate_images',
                                                             'sourceKind': 'value',
                                                             'sources': ['image-wire'],
                                                             'tests': ['pi-ai-providers/openrouter_images#complete_pinned_image_source_matches_payload_fetch_callback_and_partial_result_stages@wire-openrouter-images',
                                                                       'pi-ai-providers/openrouter_images#callback_cancellation_does_not_add_a_success_veto_and_errors_keep_partial_output@wire-openrouter-images',
                                                                       'pi-ai-providers/openrouter_images#inherited_numeric_admission_is_checked_without_changing_usage_math@wire-openrouter-images',
                                                                       'pi-ai-providers/images_runtime#wire_integration::complete_runtime_auth_dispatch_to_accepted_wire_preserves_endpoint_and_callback_cancel_success@wire-openrouter-images']},
 'packages/ai/src/auth/context.ts:defaultProviderAuthContext': {'occurrences': [{'key': './compat:defaultProviderAuthContext',
                                                                                 'routeFeatures': ['compat'],
                                                                                 'routePackage': 'pi-ai',
                                                                                 'routeProbe': 'fn '
                                                                                               'probe(){let '
                                                                                               '_: '
                                                                                               'fn(Arc<dyn '
                                                                                               'AuthContext>)->auth::DefaultProviderAuthContext '
                                                                                               '= $target;}',
                                                                                 'routeTarget': 'pi_ai::compat::DefaultProviderAuthContext::new',
                                                                                 'sources': ['auth-foundation'],
                                                                                 'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                                           'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                           'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                           'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                                {'key': '.:defaultProviderAuthContext',
                                                                                 'routeFeatures': ['compat'],
                                                                                 'routePackage': 'pi-ai',
                                                                                 'routeProbe': 'fn '
                                                                                               'probe(){let '
                                                                                               '_: '
                                                                                               'fn(Arc<dyn '
                                                                                               'AuthContext>)->auth::DefaultProviderAuthContext '
                                                                                               '= $target;}',
                                                                                 'routeTarget': 'pi_ai::DefaultProviderAuthContext::new',
                                                                                 'sources': ['auth-foundation'],
                                                                                 'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                                           'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@']}],
                                                                'owningFeatures': [],
                                                                'owningPackage': 'pi-ai-providers',
                                                                'owningProbe': 'fn probe(){let _: fn(Arc<dyn '
                                                                               'AuthContext>)->auth::DefaultProviderAuthContext '
                                                                               '= $target;}',
                                                                'owningTarget': 'pi_ai_providers::auth::DefaultProviderAuthContext::new',
                                                                'sourceKind': 'value',
                                                                'sources': ['auth-foundation'],
                                                                'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@']},
 'packages/ai/src/auth/helpers.ts:envApiKeyAuth': {'occurrences': [{'key': './compat:envApiKeyAuth',
                                                                    'routeFeatures': ['compat'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'fn probe(){let '
                                                                                  'value:auth::EnvApiKeyAuth=$target(String::new(),Vec::new()); '
                                                                                  'let _: &dyn '
                                                                                  'ApiKeyAuth=&value;}',
                                                                    'routeTarget': 'pi_ai::compat::env_api_key_auth',
                                                                    'sources': ['auth-foundation'],
                                                                    'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                              'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                   {'key': '.:envApiKeyAuth',
                                                                    'routeFeatures': ['compat'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'fn probe(){let '
                                                                                  'value:auth::EnvApiKeyAuth=$target(String::new(),Vec::new()); '
                                                                                  'let _: &dyn '
                                                                                  'ApiKeyAuth=&value;}',
                                                                    'routeTarget': 'pi_ai::env_api_key_auth',
                                                                    'sources': ['auth-foundation'],
                                                                    'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                              'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@']}],
                                                   'owningFeatures': [],
                                                   'owningPackage': 'pi-ai-providers',
                                                   'owningProbe': 'fn probe(){let '
                                                                  'value:auth::EnvApiKeyAuth=$target(String::new(),Vec::new()); '
                                                                  'let _: &dyn ApiKeyAuth=&value;}',
                                                   'owningTarget': 'pi_ai_providers::auth::env_api_key_auth',
                                                   'sourceKind': 'value',
                                                   'sources': ['auth-foundation'],
                                                   'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                             'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                             'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                             'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                             'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                             'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@']},
 'packages/ai/src/auth/helpers.ts:lazyOAuth': {'occurrences': [{'key': './compat:lazyOAuth',
                                                                'routeFeatures': ['compat'],
                                                                'routePackage': 'pi-ai',
                                                                'routeProbe': 'fn probe(){let _: '
                                                                              'fn(auth::LazyOAuthMetadata,Arc<dyn '
                                                                              'auth::OAuthLoader>,Arc<dyn '
                                                                              'TaskSpawner>)->auth::LazyOAuth '
                                                                              '= $target;}',
                                                                'routeTarget': 'pi_ai::compat::LazyOAuth::new',
                                                                'sources': ['auth-foundation'],
                                                                'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                          'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                               {'key': '.:lazyOAuth',
                                                                'routeFeatures': ['compat'],
                                                                'routePackage': 'pi-ai',
                                                                'routeProbe': 'fn probe(){let _: '
                                                                              'fn(auth::LazyOAuthMetadata,Arc<dyn '
                                                                              'auth::OAuthLoader>,Arc<dyn '
                                                                              'TaskSpawner>)->auth::LazyOAuth '
                                                                              '= $target;}',
                                                                'routeTarget': 'pi_ai::LazyOAuth::new',
                                                                'sources': ['auth-foundation'],
                                                                'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                          'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@']}],
                                               'owningFeatures': [],
                                               'owningPackage': 'pi-ai-providers',
                                               'owningProbe': 'fn probe(){let _: '
                                                              'fn(auth::LazyOAuthMetadata,Arc<dyn '
                                                              'auth::OAuthLoader>,Arc<dyn '
                                                              'TaskSpawner>)->auth::LazyOAuth = $target;}',
                                               'owningTarget': 'pi_ai_providers::auth::LazyOAuth::new',
                                               'sourceKind': 'value',
                                               'sources': ['auth-foundation'],
                                               'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                         'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                         'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@']},
 'packages/ai/src/bedrock-provider.ts:bedrockProviderModule': {'occurrences': [{'key': './bedrock-provider:bedrockProviderModule',
                                                                                'routeFeatures': ['compat'],
                                                                                'routePackage': 'pi-ai',
                                                                                'routeProbe': 'fn '
                                                                                              'probe(){let '
                                                                                              '_: '
                                                                                              'fn(&pi_ai::compat::LegacyApiAliases)->Arc<pi_ai_providers::api::bedrock_converse_stream::BedrockApi> '
                                                                                              '= $target;}',
                                                                                'routeTarget': 'pi_ai::compat::LegacyApiAliases::bedrock_provider_module',
                                                                                'sources': ['facade'],
                                                                                'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                          'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                               'owningFeatures': ['compat'],
                                                               'owningPackage': 'pi-ai',
                                                               'owningProbe': 'fn probe(){let _: '
                                                                              'fn(&pi_ai::compat::LegacyApiAliases)->Arc<pi_ai_providers::api::bedrock_converse_stream::BedrockApi> '
                                                                              '= $target;}',
                                                               'owningTarget': 'pi_ai::compat::LegacyApiAliases::bedrock_provider_module',
                                                               'sourceKind': 'value',
                                                               'sources': ['facade'],
                                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:ApiProvider': {'occurrences': [{'key': './compat:ApiProvider',
                                                            'routeFeatures': ['compat'],
                                                            'routePackage': 'pi-ai',
                                                            'routeProbe': 'fn probe(a:Arc<dyn '
                                                                          'ApiAdapter>){let '
                                                                          'v=$target::new(a,None);let _: '
                                                                          '&ApiId=v.api_id();let '
                                                                          '_:Option<&str>=v.source_id();}',
                                                            'routeTarget': 'pi_ai::compat::CompatApiBinding',
                                                            'sources': ['facade'],
                                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                      'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                           'owningFeatures': ['compat'],
                                           'owningPackage': 'pi-ai',
                                           'owningProbe': 'fn probe(a:Arc<dyn ApiAdapter>){let '
                                                          'v=$target::new(a,None);let _: '
                                                          '&ApiId=v.api_id();let '
                                                          '_:Option<&str>=v.source_id();}',
                                           'owningTarget': 'pi_ai::compat::CompatApiBinding',
                                           'sourceKind': 'type',
                                           'sources': ['facade'],
                                           'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                     'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                     'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:ApiStreamFunction': {'occurrences': [{'key': './compat:ApiStreamFunction',
                                                                  'routeFeatures': ['compat'],
                                                                  'routePackage': 'pi-ai',
                                                                  'routeProbe': 'fn probe(){let _: '
                                                                                'fn(&pi_ai::compat::CompatApiBinding,TextStreamRequest)->Result<AssistantStream,ModelsError> '
                                                                                '= $target;}',
                                                                  'routeTarget': 'pi_ai::compat::CompatApiBinding::stream',
                                                                  'sources': ['facade'],
                                                                  'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                            'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                 'owningFeatures': ['compat'],
                                                 'owningPackage': 'pi-ai',
                                                 'owningProbe': 'fn probe(){let _: '
                                                                'fn(&pi_ai::compat::CompatApiBinding,TextStreamRequest)->Result<AssistantStream,ModelsError> '
                                                                '= $target;}',
                                                 'owningTarget': 'pi_ai::compat::CompatApiBinding::stream',
                                                 'sourceKind': 'type',
                                                 'sources': ['facade'],
                                                 'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                           'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                           'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:ApiStreamSimpleFunction': {'occurrences': [{'key': './compat:ApiStreamSimpleFunction',
                                                                        'routeFeatures': ['compat'],
                                                                        'routePackage': 'pi-ai',
                                                                        'routeProbe': 'fn probe(){let _: '
                                                                                      'fn(&pi_ai::compat::CompatApiBinding,SimpleStreamRequest)->Result<AssistantStream,ModelsError> '
                                                                                      '= $target;}',
                                                                        'routeTarget': 'pi_ai::compat::CompatApiBinding::stream_simple',
                                                                        'sources': ['facade'],
                                                                        'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                  'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                       'owningFeatures': ['compat'],
                                                       'owningPackage': 'pi-ai',
                                                       'owningProbe': 'fn probe(){let _: '
                                                                      'fn(&pi_ai::compat::CompatApiBinding,SimpleStreamRequest)->Result<AssistantStream,ModelsError> '
                                                                      '= $target;}',
                                                       'owningTarget': 'pi_ai::compat::CompatApiBinding::stream_simple',
                                                       'sourceKind': 'type',
                                                       'sources': ['facade'],
                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:complete': {'occurrences': [{'key': './compat:complete',
                                                         'routeFeatures': ['compat'],
                                                         'routePackage': 'pi-ai',
                                                         'routeProbe': 'fn probe(){let _: '
                                                                       "fn(&pi_ai::compat::CompatFacade,TextStreamRequest)->BoxFuture<'static,Result<AssistantMessage,ModelsError>> "
                                                                       '= $target;}',
                                                         'routeTarget': 'pi_ai::compat::CompatFacade::complete',
                                                         'sources': ['facade'],
                                                         'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                   'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                        'owningFeatures': ['compat'],
                                        'owningPackage': 'pi-ai',
                                        'owningProbe': 'fn probe(){let _: '
                                                       "fn(&pi_ai::compat::CompatFacade,TextStreamRequest)->BoxFuture<'static,Result<AssistantMessage,ModelsError>> "
                                                       '= $target;}',
                                        'owningTarget': 'pi_ai::compat::CompatFacade::complete',
                                        'sourceKind': 'value',
                                        'sources': ['facade'],
                                        'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:completeSimple': {'occurrences': [{'key': './compat:completeSimple',
                                                               'routeFeatures': ['compat'],
                                                               'routePackage': 'pi-ai',
                                                               'routeProbe': 'fn probe(){let _: '
                                                                             "fn(&pi_ai::compat::CompatFacade,SimpleStreamRequest)->BoxFuture<'static,Result<AssistantMessage,ModelsError>> "
                                                                             '= $target;}',
                                                               'routeTarget': 'pi_ai::compat::CompatFacade::complete_simple',
                                                               'sources': ['facade'],
                                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                              'owningFeatures': ['compat'],
                                              'owningPackage': 'pi-ai',
                                              'owningProbe': 'fn probe(){let _: '
                                                             "fn(&pi_ai::compat::CompatFacade,SimpleStreamRequest)->BoxFuture<'static,Result<AssistantMessage,ModelsError>> "
                                                             '= $target;}',
                                              'owningTarget': 'pi_ai::compat::CompatFacade::complete_simple',
                                              'sourceKind': 'value',
                                              'sources': ['facade'],
                                              'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                        'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                        'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:getApiProvider': {'occurrences': [{'key': './compat:getApiProvider',
                                                               'routeFeatures': ['compat'],
                                                               'routePackage': 'pi-ai',
                                                               'routeProbe': 'fn probe(){let _: '
                                                                             'fn(&pi_ai::compat::CompatApiRegistry,&ApiId)->Option<Arc<pi_ai::compat::CompatApiBinding>> '
                                                                             '= $target;}',
                                                               'routeTarget': 'pi_ai::compat::CompatApiRegistry::get_api_provider',
                                                               'sources': ['facade'],
                                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                              'owningFeatures': ['compat'],
                                              'owningPackage': 'pi-ai',
                                              'owningProbe': 'fn probe(){let _: '
                                                             'fn(&pi_ai::compat::CompatApiRegistry,&ApiId)->Option<Arc<pi_ai::compat::CompatApiBinding>> '
                                                             '= $target;}',
                                              'owningTarget': 'pi_ai::compat::CompatApiRegistry::get_api_provider',
                                              'sourceKind': 'value',
                                              'sources': ['facade'],
                                              'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                        'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                        'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:getApiProviders': {'occurrences': [{'key': './compat:getApiProviders',
                                                                'routeFeatures': ['compat'],
                                                                'routePackage': 'pi-ai',
                                                                'routeProbe': 'fn probe(){let _: '
                                                                              'fn(&pi_ai::compat::CompatApiRegistry)->Vec<Arc<pi_ai::compat::CompatApiBinding>> '
                                                                              '= $target;}',
                                                                'routeTarget': 'pi_ai::compat::CompatApiRegistry::get_api_providers',
                                                                'sources': ['facade'],
                                                                'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                          'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                               'owningFeatures': ['compat'],
                                               'owningPackage': 'pi-ai',
                                               'owningProbe': 'fn probe(){let _: '
                                                              'fn(&pi_ai::compat::CompatApiRegistry)->Vec<Arc<pi_ai::compat::CompatApiBinding>> '
                                                              '= $target;}',
                                               'owningTarget': 'pi_ai::compat::CompatApiRegistry::get_api_providers',
                                               'sourceKind': 'value',
                                               'sources': ['facade'],
                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:getModel': {'occurrences': [{'key': './compat:getModel',
                                                         'routeFeatures': ['compat'],
                                                         'routePackage': 'pi-ai',
                                                         'routeProbe': 'fn probe(){let _: '
                                                                       'fn(&str,&str)->Result<Option<Model>,pi_ai::catalog::CatalogError> '
                                                                       '= $target;}',
                                                         'routeTarget': 'pi_ai::compat::get_model',
                                                         'sources': ['facade'],
                                                         'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                   'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                        'owningFeatures': ['catalog-openai'],
                                        'owningPackage': 'pi-ai',
                                        'owningProbe': 'fn probe(){let _: '
                                                       'fn(&str,&str)->Result<Option<Model>,pi_ai::catalog::CatalogError> '
                                                       '= $target;}',
                                        'owningTarget': 'pi_ai::catalog::get_builtin_model',
                                        'sourceKind': 'value',
                                        'sources': ['facade'],
                                        'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:getModels': {'occurrences': [{'key': './compat:getModels',
                                                          'routeFeatures': ['compat'],
                                                          'routePackage': 'pi-ai',
                                                          'routeProbe': 'fn probe(){let _: '
                                                                        'fn(&str)->Result<Vec<Model>,pi_ai::catalog::CatalogError> '
                                                                        '= $target;}',
                                                          'routeTarget': 'pi_ai::compat::get_models',
                                                          'sources': ['facade'],
                                                          'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                    'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                         'owningFeatures': ['catalog-openai'],
                                         'owningPackage': 'pi-ai',
                                         'owningProbe': 'fn probe(){let _: '
                                                        'fn(&str)->Result<Vec<Model>,pi_ai::catalog::CatalogError> '
                                                        '= $target;}',
                                         'owningTarget': 'pi_ai::catalog::get_builtin_models',
                                         'sourceKind': 'value',
                                         'sources': ['facade'],
                                         'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:getProviders': {'occurrences': [{'key': './compat:getProviders',
                                                             'routeFeatures': ['compat'],
                                                             'routePackage': 'pi-ai',
                                                             'routeProbe': 'fn probe(){let _: '
                                                                           'fn()->Vec<pi_ai::catalog::BuiltinProvider> '
                                                                           '= $target;}',
                                                             'routeTarget': 'pi_ai::compat::get_providers',
                                                             'sources': ['facade'],
                                                             'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                       'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                            'owningFeatures': ['catalog-openai'],
                                            'owningPackage': 'pi-ai',
                                            'owningProbe': 'fn probe(){let _: '
                                                           'fn()->Vec<pi_ai::catalog::BuiltinProvider> = '
                                                           '$target;}',
                                            'owningTarget': 'pi_ai::catalog::get_builtin_providers',
                                            'sourceKind': 'value',
                                            'sources': ['facade'],
                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:registerApiProvider': {'occurrences': [{'key': './compat:registerApiProvider',
                                                                    'routeFeatures': ['compat'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'fn probe(){let _: '
                                                                                  'fn(&pi_ai::compat::CompatApiRegistry,Arc<dyn '
                                                                                  'ApiAdapter>,Option<String>) '
                                                                                  '= $target;}',
                                                                    'routeTarget': 'pi_ai::compat::CompatApiRegistry::register_api_provider',
                                                                    'sources': ['facade'],
                                                                    'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                              'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                   'owningFeatures': ['compat'],
                                                   'owningPackage': 'pi-ai',
                                                   'owningProbe': 'fn probe(){let _: '
                                                                  'fn(&pi_ai::compat::CompatApiRegistry,Arc<dyn '
                                                                  'ApiAdapter>,Option<String>) = $target;}',
                                                   'owningTarget': 'pi_ai::compat::CompatApiRegistry::register_api_provider',
                                                   'sourceKind': 'value',
                                                   'sources': ['facade'],
                                                   'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                             'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                             'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:registerBuiltInApiProviders': {'occurrences': [{'key': './compat:registerBuiltInApiProviders',
                                                                            'routeFeatures': ['compat'],
                                                                            'routePackage': 'pi-ai',
                                                                            'routeProbe': 'fn probe(){let _: '
                                                                                          'fn(&pi_ai::compat::CompatApiRegistry,&pi_ai::compat::CompatApiModules) '
                                                                                          '= $target;}',
                                                                            'routeTarget': 'pi_ai::compat::CompatApiRegistry::register_builtin_api_providers',
                                                                            'sources': ['facade'],
                                                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                      'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                           'owningFeatures': ['compat'],
                                                           'owningPackage': 'pi-ai',
                                                           'owningProbe': 'fn probe(){let _: '
                                                                          'fn(&pi_ai::compat::CompatApiRegistry,&pi_ai::compat::CompatApiModules) '
                                                                          '= $target;}',
                                                           'owningTarget': 'pi_ai::compat::CompatApiRegistry::register_builtin_api_providers',
                                                           'sourceKind': 'value',
                                                           'sources': ['facade'],
                                                           'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                     'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                     'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:resetApiProviders': {'occurrences': [{'key': './compat:resetApiProviders',
                                                                  'routeFeatures': ['compat'],
                                                                  'routePackage': 'pi-ai',
                                                                  'routeProbe': 'fn probe(){let _: '
                                                                                'fn(&pi_ai::compat::CompatApiRegistry,&pi_ai::compat::CompatApiModules) '
                                                                                '= $target;}',
                                                                  'routeTarget': 'pi_ai::compat::CompatApiRegistry::reset_api_providers',
                                                                  'sources': ['facade'],
                                                                  'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                            'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                 'owningFeatures': ['compat'],
                                                 'owningPackage': 'pi-ai',
                                                 'owningProbe': 'fn probe(){let _: '
                                                                'fn(&pi_ai::compat::CompatApiRegistry,&pi_ai::compat::CompatApiModules) '
                                                                '= $target;}',
                                                 'owningTarget': 'pi_ai::compat::CompatApiRegistry::reset_api_providers',
                                                 'sourceKind': 'value',
                                                 'sources': ['facade'],
                                                 'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                           'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                           'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:stream': {'occurrences': [{'key': './compat:stream',
                                                       'routeFeatures': ['compat'],
                                                       'routePackage': 'pi-ai',
                                                       'routeProbe': 'fn probe(){let _: '
                                                                     'fn(&pi_ai::compat::CompatFacade,TextStreamRequest)->Result<AssistantStream,ModelsError> '
                                                                     '= $target;}',
                                                       'routeTarget': 'pi_ai::compat::CompatFacade::stream',
                                                       'sources': ['facade'],
                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                      'owningFeatures': ['compat'],
                                      'owningPackage': 'pi-ai',
                                      'owningProbe': 'fn probe(){let _: '
                                                     'fn(&pi_ai::compat::CompatFacade,TextStreamRequest)->Result<AssistantStream,ModelsError> '
                                                     '= $target;}',
                                      'owningTarget': 'pi_ai::compat::CompatFacade::stream',
                                      'sourceKind': 'value',
                                      'sources': ['facade'],
                                      'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:streamSimple': {'occurrences': [{'key': './compat:streamSimple',
                                                             'routeFeatures': ['compat'],
                                                             'routePackage': 'pi-ai',
                                                             'routeProbe': 'fn probe(){let _: '
                                                                           'fn(&pi_ai::compat::CompatFacade,SimpleStreamRequest)->Result<AssistantStream,ModelsError> '
                                                                           '= $target;}',
                                                             'routeTarget': 'pi_ai::compat::CompatFacade::stream_simple',
                                                             'sources': ['facade'],
                                                             'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                       'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                            'owningFeatures': ['compat'],
                                            'owningPackage': 'pi-ai',
                                            'owningProbe': 'fn probe(){let _: '
                                                           'fn(&pi_ai::compat::CompatFacade,SimpleStreamRequest)->Result<AssistantStream,ModelsError> '
                                                           '= $target;}',
                                            'owningTarget': 'pi_ai::compat::CompatFacade::stream_simple',
                                            'sourceKind': 'value',
                                            'sources': ['facade'],
                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat.ts:unregisterApiProviders': {'occurrences': [{'key': './compat:unregisterApiProviders',
                                                                       'routeFeatures': ['compat'],
                                                                       'routePackage': 'pi-ai',
                                                                       'routeProbe': 'fn probe(){let _: '
                                                                                     'fn(&pi_ai::compat::CompatApiRegistry,&str) '
                                                                                     '= $target;}',
                                                                       'routeTarget': 'pi_ai::compat::CompatApiRegistry::unregister_api_providers',
                                                                       'sources': ['facade'],
                                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                      'owningFeatures': ['compat'],
                                                      'owningPackage': 'pi-ai',
                                                      'owningProbe': 'fn probe(){let _: '
                                                                     'fn(&pi_ai::compat::CompatApiRegistry,&str) '
                                                                     '= $target;}',
                                                      'owningTarget': 'pi_ai::compat::CompatApiRegistry::unregister_api_providers',
                                                      'sourceKind': 'value',
                                                      'sources': ['facade'],
                                                      'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthAuthInfo': {'occurrences': [{'key': './compat:OAuthAuthInfo',
                                                                                    'routeFeatures': ['compat'],
                                                                                    'routePackage': 'pi-ai',
                                                                                    'routeProbe': 'fn '
                                                                                                  'probe(){let '
                                                                                                  'value=$target{url:String::new(),instructions:Some(String::new())}; '
                                                                                                  'let _ = '
                                                                                                  'value;}',
                                                                                    'routeTarget': 'pi_ai::compat::OAuthAuthInfo',
                                                                                    'sources': ['oauth-types'],
                                                                                    'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                   {'key': './oauth:OAuthAuthInfo',
                                                                                    'routeFeatures': ['compat'],
                                                                                    'routePackage': 'pi-ai',
                                                                                    'routeProbe': 'fn '
                                                                                                  'probe(){let '
                                                                                                  'value=$target{url:String::new(),instructions:Some(String::new())}; '
                                                                                                  'let _ = '
                                                                                                  'value;}',
                                                                                    'routeTarget': 'pi_ai::compat::OAuthAuthInfo',
                                                                                    'sources': ['oauth-types'],
                                                                                    'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                   {'key': '.:OAuthAuthInfo',
                                                                                    'routeFeatures': ['compat'],
                                                                                    'routePackage': 'pi-ai',
                                                                                    'routeProbe': 'fn '
                                                                                                  'probe(){let '
                                                                                                  'value=$target{url:String::new(),instructions:Some(String::new())}; '
                                                                                                  'let _ = '
                                                                                                  'value;}',
                                                                                    'routeTarget': 'pi_ai::OAuthAuthInfo',
                                                                                    'sources': ['oauth-types'],
                                                                                    'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                              'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                   'owningFeatures': ['compat'],
                                                                   'owningPackage': 'pi-ai',
                                                                   'owningProbe': 'fn probe(){let '
                                                                                  'value=$target{url:String::new(),instructions:Some(String::new())}; '
                                                                                  'let _ = value;}',
                                                                   'owningTarget': 'pi_ai::compat::OAuthAuthInfo',
                                                                   'sourceKind': 'type',
                                                                   'sources': ['oauth-types'],
                                                                   'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                             'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                             'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                             'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                             'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthCredentials': {'occurrences': [{'key': './oauth:OAuthCredentials',
                                                                                       'routeFeatures': ['compat'],
                                                                                       'routePackage': 'pi-ai',
                                                                                       'routeProbe': 'fn '
                                                                                                     'probe(v:pi_ai_core::OAuthCredentials){let '
                                                                                                     '_: '
                                                                                                     '$target=v;}',
                                                                                       'routeTarget': 'pi_ai::compat::OAuthCredentials',
                                                                                       'sources': ['oauth-types'],
                                                                                       'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                 'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                 'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                 'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                 'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                      'owningFeatures': ['compat'],
                                                                      'owningPackage': 'pi-ai',
                                                                      'owningProbe': 'fn '
                                                                                     'probe(v:pi_ai_core::OAuthCredentials){let '
                                                                                     '_: $target=v;}',
                                                                      'owningTarget': 'pi_ai::compat::OAuthCredentials',
                                                                      'sourceKind': 'type',
                                                                      'sources': ['oauth-types'],
                                                                      'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthDeviceCodeInfo': {'occurrences': [{'key': './compat:OAuthDeviceCodeInfo',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'fn '
                                                                                                        'probe(){let '
                                                                                                        'value=$target{user_code:String::new(),verification_uri:String::new(),interval_seconds:Some(SafeUnsigned::new(0).unwrap()),expires_in_seconds:None}; '
                                                                                                        'let '
                                                                                                        '_ = '
                                                                                                        'value;}',
                                                                                          'routeTarget': 'pi_ai::compat::OAuthDeviceCodeInfo',
                                                                                          'sources': ['oauth-types'],
                                                                                          'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                         {'key': './oauth:OAuthDeviceCodeInfo',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'fn '
                                                                                                        'probe(){let '
                                                                                                        'value=$target{user_code:String::new(),verification_uri:String::new(),interval_seconds:Some(SafeUnsigned::new(0).unwrap()),expires_in_seconds:None}; '
                                                                                                        'let '
                                                                                                        '_ = '
                                                                                                        'value;}',
                                                                                          'routeTarget': 'pi_ai::compat::OAuthDeviceCodeInfo',
                                                                                          'sources': ['oauth-types'],
                                                                                          'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                         {'key': '.:OAuthDeviceCodeInfo',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'fn '
                                                                                                        'probe(){let '
                                                                                                        'value=$target{user_code:String::new(),verification_uri:String::new(),interval_seconds:Some(SafeUnsigned::new(0).unwrap()),expires_in_seconds:None}; '
                                                                                                        'let '
                                                                                                        '_ = '
                                                                                                        'value;}',
                                                                                          'routeTarget': 'pi_ai::OAuthDeviceCodeInfo',
                                                                                          'sources': ['oauth-types'],
                                                                                          'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                         'owningFeatures': ['compat'],
                                                                         'owningPackage': 'pi-ai',
                                                                         'owningProbe': 'fn probe(){let '
                                                                                        'value=$target{user_code:String::new(),verification_uri:String::new(),interval_seconds:Some(SafeUnsigned::new(0).unwrap()),expires_in_seconds:None}; '
                                                                                        'let _ = value;}',
                                                                         'owningTarget': 'pi_ai::compat::OAuthDeviceCodeInfo',
                                                                         'sourceKind': 'type',
                                                                         'sources': ['oauth-types'],
                                                                         'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthLoginCallbacks': {'occurrences': [{'key': './compat:OAuthLoginCallbacks',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'use '
                                                                                                        '$target '
                                                                                                        'as '
                                                                                                        'Required;struct '
                                                                                                        'Probe;impl '
                                                                                                        'Required '
                                                                                                        'for '
                                                                                                        'Probe{fn '
                                                                                                        'on_auth(&self,_:pi_ai::compat::OAuthAuthInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                                        'on_device_code(&self,_:pi_ai::compat::OAuthDeviceCodeInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                                        "on_prompt<'a>(&'a "
                                                                                                        "self,_:pi_ai::compat::OAuthPrompt)->BoxFuture<'a,Result<String,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                                        "on_select<'a>(&'a "
                                                                                                        "self,_:pi_ai::compat::OAuthSelectPrompt)->BoxFuture<'a,Result<Option<String>,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                                        'progress(&self)->Option<&(dyn '
                                                                                                        "pi_ai::compat::OAuthProgressCallback+'_)>{None}fn "
                                                                                                        'manual_code_input(&self)->Option<&(dyn '
                                                                                                        "pi_ai::compat::OAuthManualCodeInputCallback+'_)>{None}fn "
                                                                                                        'signal(&self)->Option<&CancellationToken>{None}}fn '
                                                                                                        'probe(p:&Probe){let '
                                                                                                        '_: '
                                                                                                        '&dyn '
                                                                                                        'Required=p;}',
                                                                                          'routeTarget': 'pi_ai::compat::OAuthLoginCallbacks',
                                                                                          'sources': ['oauth-types'],
                                                                                          'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                         {'key': './oauth:OAuthLoginCallbacks',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'use '
                                                                                                        '$target '
                                                                                                        'as '
                                                                                                        'Required;struct '
                                                                                                        'Probe;impl '
                                                                                                        'Required '
                                                                                                        'for '
                                                                                                        'Probe{fn '
                                                                                                        'on_auth(&self,_:pi_ai::compat::OAuthAuthInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                                        'on_device_code(&self,_:pi_ai::compat::OAuthDeviceCodeInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                                        "on_prompt<'a>(&'a "
                                                                                                        "self,_:pi_ai::compat::OAuthPrompt)->BoxFuture<'a,Result<String,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                                        "on_select<'a>(&'a "
                                                                                                        "self,_:pi_ai::compat::OAuthSelectPrompt)->BoxFuture<'a,Result<Option<String>,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                                        'progress(&self)->Option<&(dyn '
                                                                                                        "pi_ai::compat::OAuthProgressCallback+'_)>{None}fn "
                                                                                                        'manual_code_input(&self)->Option<&(dyn '
                                                                                                        "pi_ai::compat::OAuthManualCodeInputCallback+'_)>{None}fn "
                                                                                                        'signal(&self)->Option<&CancellationToken>{None}}fn '
                                                                                                        'probe(p:&Probe){let '
                                                                                                        '_: '
                                                                                                        '&dyn '
                                                                                                        'Required=p;}',
                                                                                          'routeTarget': 'pi_ai::compat::OAuthLoginCallbacks',
                                                                                          'sources': ['oauth-types'],
                                                                                          'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                         {'key': '.:OAuthLoginCallbacks',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'use '
                                                                                                        '$target '
                                                                                                        'as '
                                                                                                        'Required;struct '
                                                                                                        'Probe;impl '
                                                                                                        'Required '
                                                                                                        'for '
                                                                                                        'Probe{fn '
                                                                                                        'on_auth(&self,_:pi_ai::compat::OAuthAuthInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                                        'on_device_code(&self,_:pi_ai::compat::OAuthDeviceCodeInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                                        "on_prompt<'a>(&'a "
                                                                                                        "self,_:pi_ai::compat::OAuthPrompt)->BoxFuture<'a,Result<String,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                                        "on_select<'a>(&'a "
                                                                                                        "self,_:pi_ai::compat::OAuthSelectPrompt)->BoxFuture<'a,Result<Option<String>,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                                        'progress(&self)->Option<&(dyn '
                                                                                                        "pi_ai::compat::OAuthProgressCallback+'_)>{None}fn "
                                                                                                        'manual_code_input(&self)->Option<&(dyn '
                                                                                                        "pi_ai::compat::OAuthManualCodeInputCallback+'_)>{None}fn "
                                                                                                        'signal(&self)->Option<&CancellationToken>{None}}fn '
                                                                                                        'probe(p:&Probe){let '
                                                                                                        '_: '
                                                                                                        '&dyn '
                                                                                                        'Required=p;}',
                                                                                          'routeTarget': 'pi_ai::OAuthLoginCallbacks',
                                                                                          'sources': ['oauth-types'],
                                                                                          'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                    'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                         'owningFeatures': ['compat'],
                                                                         'owningPackage': 'pi-ai',
                                                                         'owningProbe': 'use $target as '
                                                                                        'Required;struct '
                                                                                        'Probe;impl Required '
                                                                                        'for Probe{fn '
                                                                                        'on_auth(&self,_:pi_ai::compat::OAuthAuthInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                        'on_device_code(&self,_:pi_ai::compat::OAuthDeviceCodeInfo)->Result<(),AuthInteractionError>{Ok(())}fn '
                                                                                        "on_prompt<'a>(&'a "
                                                                                        "self,_:pi_ai::compat::OAuthPrompt)->BoxFuture<'a,Result<String,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                        "on_select<'a>(&'a "
                                                                                        "self,_:pi_ai::compat::OAuthSelectPrompt)->BoxFuture<'a,Result<Option<String>,AuthInteractionError>>{Box::pin(std::future::pending())}fn "
                                                                                        'progress(&self)->Option<&(dyn '
                                                                                        "pi_ai::compat::OAuthProgressCallback+'_)>{None}fn "
                                                                                        'manual_code_input(&self)->Option<&(dyn '
                                                                                        "pi_ai::compat::OAuthManualCodeInputCallback+'_)>{None}fn "
                                                                                        'signal(&self)->Option<&CancellationToken>{None}}fn '
                                                                                        'probe(p:&Probe){let '
                                                                                        '_: &dyn '
                                                                                        'Required=p;}',
                                                                         'owningTarget': 'pi_ai::compat::OAuthLoginCallbacks',
                                                                         'sourceKind': 'type',
                                                                         'sources': ['oauth-types'],
                                                                         'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                   'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthPrompt': {'occurrences': [{'key': './compat:OAuthPrompt',
                                                                                  'routeFeatures': ['compat'],
                                                                                  'routePackage': 'pi-ai',
                                                                                  'routeProbe': 'fn '
                                                                                                'probe(){let '
                                                                                                'value=$target{message:String::new(),placeholder:Some(String::new()),allow_empty:Some(false)}; '
                                                                                                'let _ = '
                                                                                                'value;}',
                                                                                  'routeTarget': 'pi_ai::compat::OAuthPrompt',
                                                                                  'sources': ['oauth-types'],
                                                                                  'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                 {'key': './oauth:OAuthPrompt',
                                                                                  'routeFeatures': ['compat'],
                                                                                  'routePackage': 'pi-ai',
                                                                                  'routeProbe': 'fn '
                                                                                                'probe(){let '
                                                                                                'value=$target{message:String::new(),placeholder:Some(String::new()),allow_empty:Some(false)}; '
                                                                                                'let _ = '
                                                                                                'value;}',
                                                                                  'routeTarget': 'pi_ai::compat::OAuthPrompt',
                                                                                  'sources': ['oauth-types'],
                                                                                  'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                 {'key': '.:OAuthPrompt',
                                                                                  'routeFeatures': ['compat'],
                                                                                  'routePackage': 'pi-ai',
                                                                                  'routeProbe': 'fn '
                                                                                                'probe(){let '
                                                                                                'value=$target{message:String::new(),placeholder:Some(String::new()),allow_empty:Some(false)}; '
                                                                                                'let _ = '
                                                                                                'value;}',
                                                                                  'routeTarget': 'pi_ai::OAuthPrompt',
                                                                                  'sources': ['oauth-types'],
                                                                                  'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                            'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                 'owningFeatures': ['compat'],
                                                                 'owningPackage': 'pi-ai',
                                                                 'owningProbe': 'fn probe(){let '
                                                                                'value=$target{message:String::new(),placeholder:Some(String::new()),allow_empty:Some(false)}; '
                                                                                'let _ = value;}',
                                                                 'owningTarget': 'pi_ai::compat::OAuthPrompt',
                                                                 'sourceKind': 'type',
                                                                 'sources': ['oauth-types'],
                                                                 'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                           'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                           'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                           'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                           'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthSelectOption': {'occurrences': [{'key': './compat:OAuthSelectOption',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      'value=$target{id:String::new(),label:String::new()}; '
                                                                                                      'let _ '
                                                                                                      '= '
                                                                                                      'value;}',
                                                                                        'routeTarget': 'pi_ai::compat::OAuthSelectOption',
                                                                                        'sources': ['oauth-types'],
                                                                                        'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                       {'key': './oauth:OAuthSelectOption',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      'value=$target{id:String::new(),label:String::new()}; '
                                                                                                      'let _ '
                                                                                                      '= '
                                                                                                      'value;}',
                                                                                        'routeTarget': 'pi_ai::compat::OAuthSelectOption',
                                                                                        'sources': ['oauth-types'],
                                                                                        'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                       {'key': '.:OAuthSelectOption',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      'value=$target{id:String::new(),label:String::new()}; '
                                                                                                      'let _ '
                                                                                                      '= '
                                                                                                      'value;}',
                                                                                        'routeTarget': 'pi_ai::OAuthSelectOption',
                                                                                        'sources': ['oauth-types'],
                                                                                        'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                       'owningFeatures': ['compat'],
                                                                       'owningPackage': 'pi-ai',
                                                                       'owningProbe': 'fn probe(){let '
                                                                                      'value=$target{id:String::new(),label:String::new()}; '
                                                                                      'let _ = value;}',
                                                                       'owningTarget': 'pi_ai::compat::OAuthSelectOption',
                                                                       'sourceKind': 'type',
                                                                       'sources': ['oauth-types'],
                                                                       'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/compat/extension-oauth-types.ts:OAuthSelectPrompt': {'occurrences': [{'key': './compat:OAuthSelectPrompt',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      'value=$target{message:String::new(),options:Vec::new()}; '
                                                                                                      'let _ '
                                                                                                      '= '
                                                                                                      'value;}',
                                                                                        'routeTarget': 'pi_ai::compat::OAuthSelectPrompt',
                                                                                        'sources': ['oauth-types'],
                                                                                        'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                       {'key': './oauth:OAuthSelectPrompt',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      'value=$target{message:String::new(),options:Vec::new()}; '
                                                                                                      'let _ '
                                                                                                      '= '
                                                                                                      'value;}',
                                                                                        'routeTarget': 'pi_ai::compat::OAuthSelectPrompt',
                                                                                        'sources': ['oauth-types'],
                                                                                        'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
                                                                                       {'key': '.:OAuthSelectPrompt',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      'value=$target{message:String::new(),options:Vec::new()}; '
                                                                                                      'let _ '
                                                                                                      '= '
                                                                                                      'value;}',
                                                                                        'routeTarget': 'pi_ai::OAuthSelectPrompt',
                                                                                        'sources': ['oauth-types'],
                                                                                        'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                                  'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']}],
                                                                       'owningFeatures': ['compat'],
                                                                       'owningPackage': 'pi-ai',
                                                                       'owningProbe': 'fn probe(){let '
                                                                                      'value=$target{message:String::new(),options:Vec::new()}; '
                                                                                      'let _ = value;}',
                                                                       'owningTarget': 'pi_ai::compat::OAuthSelectPrompt',
                                                                       'sourceKind': 'type',
                                                                       'sources': ['oauth-types'],
                                                                       'tests': ['pi-ai/compat_oauth_types#compat_oauth_required_trait_objects_preserve_values_order_and_select_absence@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_optional_presence_is_observable_without_invocation@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_sync_and_async_errors_retain_the_callback_source@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_credentials_reexport_is_the_existing_nominal_core_value@compat',
                                                                                 'pi-ai/compat_oauth_types#compat_oauth_complete_pinned_source_declarations_and_exports@compat']},
 'packages/ai/src/env-api-keys.ts:ANTHROPIC_API_KEY_ENV': {'occurrences': [{'key': './compat:ANTHROPIC_API_KEY_ENV',
                                                                            'routeFeatures': ['compat'],
                                                                            'routePackage': 'pi-ai',
                                                                            'routeProbe': 'const _: () = { '
                                                                                          'let value: '
                                                                                          "&'static str = "
                                                                                          '$target; let '
                                                                                          'actual = '
                                                                                          'value.as_bytes(); '
                                                                                          'let expected = '
                                                                                          'b"ANTHROPIC_API_KEY"; '
                                                                                          'assert!(actual.len() '
                                                                                          '== '
                                                                                          'expected.len(), '
                                                                                          '"S05 constant '
                                                                                          'value mismatch"); '
                                                                                          'let mut i = 0; '
                                                                                          'while i < '
                                                                                          'expected.len() { '
                                                                                          'assert!(actual[i] '
                                                                                          '== expected[i], '
                                                                                          '"S05 constant '
                                                                                          'value mismatch"); '
                                                                                          'i += 1; } };',
                                                                            'routeTarget': 'pi_ai::compat::ANTHROPIC_API_KEY_ENV',
                                                                            'sources': ['legacy-env'],
                                                                            'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                                      'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                                      'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                                      'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                                      'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                                      'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                                      'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat',
                                                                                      'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                           'owningFeatures': [],
                                                           'owningPackage': 'pi-ai-providers',
                                                           'owningProbe': 'const _: () = { let value: '
                                                                          "&'static str = $target; let "
                                                                          'actual = value.as_bytes(); let '
                                                                          'expected = b"ANTHROPIC_API_KEY"; '
                                                                          'assert!(actual.len() == '
                                                                          'expected.len(), "S05 constant '
                                                                          'value mismatch"); let mut i = 0; '
                                                                          'while i < expected.len() { '
                                                                          'assert!(actual[i] == expected[i], '
                                                                          '"S05 constant value mismatch"); i '
                                                                          '+= 1; } };',
                                                           'owningTarget': 'pi_ai_providers::auth::ANTHROPIC_API_KEY_ENV',
                                                           'sourceKind': 'value',
                                                           'sources': ['legacy-env'],
                                                           'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                     'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                     'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                     'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                     'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                     'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                     'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat']},
 'packages/ai/src/env-api-keys.ts:ANTHROPIC_AUTH_TOKEN_ENV': {'occurrences': [{'key': './compat:ANTHROPIC_AUTH_TOKEN_ENV',
                                                                               'routeFeatures': ['compat'],
                                                                               'routePackage': 'pi-ai',
                                                                               'routeProbe': 'const _: () = '
                                                                                             '{ let value: '
                                                                                             "&'static str = "
                                                                                             '$target; let '
                                                                                             'actual = '
                                                                                             'value.as_bytes(); '
                                                                                             'let expected = '
                                                                                             'b"ANTHROPIC_AUTH_TOKEN"; '
                                                                                             'assert!(actual.len() '
                                                                                             '== '
                                                                                             'expected.len(), '
                                                                                             '"S05 constant '
                                                                                             'value '
                                                                                             'mismatch"); '
                                                                                             'let mut i = 0; '
                                                                                             'while i < '
                                                                                             'expected.len() '
                                                                                             '{ '
                                                                                             'assert!(actual[i] '
                                                                                             '== '
                                                                                             'expected[i], '
                                                                                             '"S05 constant '
                                                                                             'value '
                                                                                             'mismatch"); i '
                                                                                             '+= 1; } };',
                                                                               'routeTarget': 'pi_ai::compat::ANTHROPIC_AUTH_TOKEN_ENV',
                                                                               'sources': ['legacy-env'],
                                                                               'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                                         'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                                         'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                                         'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat',
                                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                              'owningFeatures': [],
                                                              'owningPackage': 'pi-ai-providers',
                                                              'owningProbe': 'const _: () = { let value: '
                                                                             "&'static str = $target; let "
                                                                             'actual = value.as_bytes(); let '
                                                                             'expected = '
                                                                             'b"ANTHROPIC_AUTH_TOKEN"; '
                                                                             'assert!(actual.len() == '
                                                                             'expected.len(), "S05 constant '
                                                                             'value mismatch"); let mut i = '
                                                                             '0; while i < expected.len() { '
                                                                             'assert!(actual[i] == '
                                                                             'expected[i], "S05 constant '
                                                                             'value mismatch"); i += 1; } };',
                                                              'owningTarget': 'pi_ai_providers::auth::ANTHROPIC_AUTH_TOKEN_ENV',
                                                              'sourceKind': 'value',
                                                              'sources': ['legacy-env'],
                                                              'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                        'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                        'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                        'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                        'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                        'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                        'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat']},
 'packages/ai/src/env-api-keys.ts:ANTHROPIC_OAUTH_TOKEN_ENV': {'occurrences': [{'key': './compat:ANTHROPIC_OAUTH_TOKEN_ENV',
                                                                                'routeFeatures': ['compat'],
                                                                                'routePackage': 'pi-ai',
                                                                                'routeProbe': 'const _: () = '
                                                                                              '{ let value: '
                                                                                              "&'static str "
                                                                                              '= $target; '
                                                                                              'let actual = '
                                                                                              'value.as_bytes(); '
                                                                                              'let expected '
                                                                                              '= '
                                                                                              'b"ANTHROPIC_OAUTH_TOKEN"; '
                                                                                              'assert!(actual.len() '
                                                                                              '== '
                                                                                              'expected.len(), '
                                                                                              '"S05 constant '
                                                                                              'value '
                                                                                              'mismatch"); '
                                                                                              'let mut i = '
                                                                                              '0; while i < '
                                                                                              'expected.len() '
                                                                                              '{ '
                                                                                              'assert!(actual[i] '
                                                                                              '== '
                                                                                              'expected[i], '
                                                                                              '"S05 constant '
                                                                                              'value '
                                                                                              'mismatch"); i '
                                                                                              '+= 1; } };',
                                                                                'routeTarget': 'pi_ai::compat::ANTHROPIC_OAUTH_TOKEN_ENV',
                                                                                'sources': ['legacy-env'],
                                                                                'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                                          'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                                          'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                                          'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                                          'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat',
                                                                                          'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                               'owningFeatures': [],
                                                               'owningPackage': 'pi-ai-providers',
                                                               'owningProbe': 'const _: () = { let value: '
                                                                              "&'static str = $target; let "
                                                                              'actual = value.as_bytes(); '
                                                                              'let expected = '
                                                                              'b"ANTHROPIC_OAUTH_TOKEN"; '
                                                                              'assert!(actual.len() == '
                                                                              'expected.len(), "S05 constant '
                                                                              'value mismatch"); let mut i = '
                                                                              '0; while i < expected.len() { '
                                                                              'assert!(actual[i] == '
                                                                              'expected[i], "S05 constant '
                                                                              'value mismatch"); i += 1; } '
                                                                              '};',
                                                               'owningTarget': 'pi_ai_providers::auth::ANTHROPIC_OAUTH_TOKEN_ENV',
                                                               'sourceKind': 'value',
                                                               'sources': ['legacy-env'],
                                                               'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                         'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                         'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                         'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                         'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat']},
 'packages/ai/src/env-api-keys.ts:findEnvKeys': {'occurrences': [{'key': './compat:findEnvKeys',
                                                                  'routeFeatures': ['compat'],
                                                                  'routePackage': 'pi-ai',
                                                                  'routeProbe': 'fn '
                                                                                "output<'a,F:Future<Output=Result<Option<Vec<String>>,AuthProviderError>>+Send+'a>(_:F){} "
                                                                                "fn probe<'a>(p:&'a "
                                                                                "str,e:Option<&'a "
                                                                                "ProviderEnv>,c:&'a dyn "
                                                                                "AuthContext,t:&'a "
                                                                                'CancellationToken){output($target(p,e,c,t));}',
                                                                  'routeTarget': 'pi_ai::compat::find_env_keys',
                                                                  'sources': ['legacy-env'],
                                                                  'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                            'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                            'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                            'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                            'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                            'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                            'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat',
                                                                            'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                 'owningFeatures': [],
                                                 'owningPackage': 'pi-ai-providers',
                                                 'owningProbe': 'fn '
                                                                "output<'a,F:Future<Output=Result<Option<Vec<String>>,AuthProviderError>>+Send+'a>(_:F){} "
                                                                "fn probe<'a>(p:&'a str,e:Option<&'a "
                                                                "ProviderEnv>,c:&'a dyn AuthContext,t:&'a "
                                                                'CancellationToken){output($target(p,e,c,t));}',
                                                 'owningTarget': 'pi_ai_providers::auth::find_env_keys',
                                                 'sourceKind': 'value',
                                                 'sources': ['legacy-env'],
                                                 'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                           'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                           'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                           'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                           'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat']},
 'packages/ai/src/env-api-keys.ts:getEnvApiKey': {'occurrences': [{'key': './compat:getEnvApiKey',
                                                                   'routeFeatures': ['compat'],
                                                                   'routePackage': 'pi-ai',
                                                                   'routeProbe': 'fn '
                                                                                 "output<'a,F:Future<Output=Result<Option<SecretString>,AuthProviderError>>+Send+'a>(_:F){} "
                                                                                 "fn probe<'a>(p:&'a "
                                                                                 "str,e:Option<&'a "
                                                                                 "ProviderEnv>,c:&'a dyn "
                                                                                 "AuthContext,t:&'a "
                                                                                 'CancellationToken){output($target(p,e,c,t));}',
                                                                   'routeTarget': 'pi_ai::compat::get_env_api_key',
                                                                   'sources': ['legacy-env'],
                                                                   'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                                             'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                                             'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                                             'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                                             'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                                             'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                                             'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat',
                                                                             'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                             'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                             'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                  'owningFeatures': [],
                                                  'owningPackage': 'pi-ai-providers',
                                                  'owningProbe': 'fn '
                                                                 "output<'a,F:Future<Output=Result<Option<SecretString>,AuthProviderError>>+Send+'a>(_:F){} "
                                                                 "fn probe<'a>(p:&'a str,e:Option<&'a "
                                                                 "ProviderEnv>,c:&'a dyn AuthContext,t:&'a "
                                                                 'CancellationToken){output($target(p,e,c,t));}',
                                                  'owningTarget': 'pi_ai_providers::auth::get_env_api_key',
                                                  'sourceKind': 'value',
                                                  'sources': ['legacy-env'],
                                                  'tests': ['pi-ai-providers/auth_foundation#auth_foundation_key_precedence_truthiness_and_cancellation@',
                                                            'pi-ai-providers/auth_foundation#auth_foundation_context_and_selected_legacy_discovery@',
                                                            'pi-ai-providers/auth_foundation#auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop@',
                                                            'pi-ai-providers/auth_foundation#auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load@',
                                                            'pi-ai-providers/auth_foundation#auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop@',
                                                            'pi-ai-providers/auth_foundation#auth_foundation_complete_pinned_source_and_hashes_remain_exact@',
                                                            'pi-ai/s05_export_legacy_env#s05_selected_legacy_env_complete_source_and_facade_results@compat']},
 'packages/ai/src/image-models.ts:getImageModel': {'occurrences': [{'key': './compat:getImageModel',
                                                                    'routeFeatures': ['compat'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'fn probe(){let _: '
                                                                                  'fn(&str,&str)->Result<Option<ImagesModel>,pi_ai::catalog::CatalogError> '
                                                                                  '= $target;}',
                                                                    'routeTarget': 'pi_ai::compat::get_image_model',
                                                                    'sources': ['facade'],
                                                                    'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                              'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                   'owningFeatures': ['catalog-openrouter-images'],
                                                   'owningPackage': 'pi-ai',
                                                   'owningProbe': 'fn probe(){let _: '
                                                                  'fn(&str,&str)->Result<Option<ImagesModel>,pi_ai::catalog::CatalogError> '
                                                                  '= $target;}',
                                                   'owningTarget': 'pi_ai::catalog::get_builtin_images_model',
                                                   'sourceKind': 'value',
                                                   'sources': ['facade'],
                                                   'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                             'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                             'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/image-models.ts:getImageModels': {'occurrences': [{'key': './compat:getImageModels',
                                                                     'routeFeatures': ['compat'],
                                                                     'routePackage': 'pi-ai',
                                                                     'routeProbe': 'fn probe(){let _: '
                                                                                   'fn(&str)->Result<Vec<ImagesModel>,pi_ai::catalog::CatalogError> '
                                                                                   '= $target;}',
                                                                     'routeTarget': 'pi_ai::compat::get_image_models',
                                                                     'sources': ['facade'],
                                                                     'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                               'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                               'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                               'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                               'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                               'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                    'owningFeatures': ['catalog-openrouter-images'],
                                                    'owningPackage': 'pi-ai',
                                                    'owningProbe': 'fn probe(){let _: '
                                                                   'fn(&str)->Result<Vec<ImagesModel>,pi_ai::catalog::CatalogError> '
                                                                   '= $target;}',
                                                    'owningTarget': 'pi_ai::catalog::get_builtin_images_models',
                                                    'sourceKind': 'value',
                                                    'sources': ['facade'],
                                                    'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/image-models.ts:getImageProviders': {'occurrences': [{'key': './compat:getImageProviders',
                                                                        'routeFeatures': ['compat'],
                                                                        'routePackage': 'pi-ai',
                                                                        'routeProbe': 'fn probe(){let _: '
                                                                                      "fn()->Vec<&'static "
                                                                                      'str> = $target;}',
                                                                        'routeTarget': 'pi_ai::compat::get_image_providers',
                                                                        'sources': ['facade'],
                                                                        'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                  'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                       'owningFeatures': ['catalog-openrouter-images'],
                                                       'owningPackage': 'pi-ai',
                                                       'owningProbe': "fn probe(){let _: fn()->Vec<&'static "
                                                                      'str> = $target;}',
                                                       'owningTarget': 'pi_ai::catalog::get_builtin_images_providers',
                                                       'sourceKind': 'value',
                                                       'sources': ['facade'],
                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/images-api-registry.ts:ImagesApiFunction': {'occurrences': [{'key': './compat:ImagesApiFunction',
                                                                               'routeFeatures': ['compat'],
                                                                               'routePackage': 'pi-ai',
                                                                               'routeProbe': 'use $target as '
                                                                                             'Required; '
                                                                                             'struct Probe; '
                                                                                             'impl Required '
                                                                                             'for Probe {fn '
                                                                                             "generate<'a>(&'a "
                                                                                             "self,_:ImageGenerationRequest)->Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>{Ok(Box::pin(std::future::pending()))}} "
                                                                                             'fn '
                                                                                             'probe(v:&Probe){let '
                                                                                             '_: &dyn '
                                                                                             'Required=v;}',
                                                                               'routeTarget': 'pi_ai::compat::ImagesGenerator',
                                                                               'sources': ['images-runtime',
                                                                                           'facade'],
                                                                               'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                         'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                         'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                         'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                         'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                         'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                         'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                         'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                         'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                         'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                         'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                              'owningFeatures': [],
                                                              'owningPackage': 'pi-ai-providers',
                                                              'owningProbe': 'use $target as Required; '
                                                                             'struct Probe; impl Required '
                                                                             "for Probe {fn generate<'a>(&'a "
                                                                             "self,_:ImageGenerationRequest)->Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>{Ok(Box::pin(std::future::pending()))}} "
                                                                             'fn probe(v:&Probe){let _: &dyn '
                                                                             'Required=v;}',
                                                              'owningTarget': 'pi_ai_providers::images::ImagesGenerator',
                                                              'sourceKind': 'type',
                                                              'sources': ['images-runtime'],
                                                              'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                        'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                        'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                        'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                        'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                        'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                        'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                        'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                        'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                        'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                        'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-api-registry.ts:ImagesApiProvider': {'occurrences': [{'key': './compat:ImagesApiProvider',
                                                                               'routeFeatures': ['compat'],
                                                                               'routePackage': 'pi-ai',
                                                                               'routeProbe': 'fn '
                                                                                             'probe(api:ImagesApiId,g:Arc<dyn '
                                                                                             'img::ImagesGenerator>,r:ImageGenerationRequest){let '
                                                                                             'v=$target::new(api,g);let '
                                                                                             '_: '
                                                                                             '&ImagesApiId=v.api_id();let '
                                                                                             "_:img::ImagesGenerateCall<'_>=v.generate(r);}",
                                                                               'routeTarget': 'pi_ai::compat::ImagesApiBinding',
                                                                               'sources': ['images-runtime',
                                                                                           'facade'],
                                                                               'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                         'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                         'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                         'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                         'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                         'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                         'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                         'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                         'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                         'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                         'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                              'owningFeatures': [],
                                                              'owningPackage': 'pi-ai-providers',
                                                              'owningProbe': 'fn '
                                                                             'probe(api:ImagesApiId,g:Arc<dyn '
                                                                             'img::ImagesGenerator>,r:ImageGenerationRequest){let '
                                                                             'v=$target::new(api,g);let _: '
                                                                             '&ImagesApiId=v.api_id();let '
                                                                             "_:img::ImagesGenerateCall<'_>=v.generate(r);}",
                                                              'owningTarget': 'pi_ai_providers::images::ImagesApiBinding',
                                                              'sourceKind': 'type',
                                                              'sources': ['images-runtime'],
                                                              'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                        'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                        'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                        'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                        'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                        'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                        'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                        'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                        'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                        'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                        'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-api-registry.ts:getImagesApiProvider': {'occurrences': [{'key': './compat:getImagesApiProvider',
                                                                                  'routeFeatures': ['compat'],
                                                                                  'routePackage': 'pi-ai',
                                                                                  'routeProbe': 'fn '
                                                                                                'probe(){let '
                                                                                                '_: '
                                                                                                'fn(&img::ImagesApiRegistry,&ImagesApiId)->Option<Arc<img::ImagesApiBinding>> '
                                                                                                '= $target;}',
                                                                                  'routeTarget': 'pi_ai::compat::ImagesApiRegistry::get',
                                                                                  'sources': ['images-runtime',
                                                                                              'facade'],
                                                                                  'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                            'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                            'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                            'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                            'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                            'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                            'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                            'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                            'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                            'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                            'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                            'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                 'owningFeatures': [],
                                                                 'owningPackage': 'pi-ai-providers',
                                                                 'owningProbe': 'fn probe(){let _: '
                                                                                'fn(&img::ImagesApiRegistry,&ImagesApiId)->Option<Arc<img::ImagesApiBinding>> '
                                                                                '= $target;}',
                                                                 'owningTarget': 'pi_ai_providers::images::ImagesApiRegistry::get',
                                                                 'sourceKind': 'value',
                                                                 'sources': ['images-runtime'],
                                                                 'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                           'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                           'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                           'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                           'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                           'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                           'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                           'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                           'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                           'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                           'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-api-registry.ts:registerImagesApiProvider': {'occurrences': [{'key': './compat:registerImagesApiProvider',
                                                                                       'routeFeatures': ['compat'],
                                                                                       'routePackage': 'pi-ai',
                                                                                       'routeProbe': 'fn '
                                                                                                     'probe(){let '
                                                                                                     '_: '
                                                                                                     'fn(&img::ImagesApiRegistry,img::ImagesApiBinding,Option<String>) '
                                                                                                     '= '
                                                                                                     '$target;}',
                                                                                       'routeTarget': 'pi_ai::compat::ImagesApiRegistry::register',
                                                                                       'sources': ['images-runtime',
                                                                                                   'facade'],
                                                                                       'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                                 'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                                 'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                                 'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                                 'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                                 'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                                 'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                                 'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                                 'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                                 'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                                 'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                                 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                      'owningFeatures': [],
                                                                      'owningPackage': 'pi-ai-providers',
                                                                      'owningProbe': 'fn probe(){let _: '
                                                                                     'fn(&img::ImagesApiRegistry,img::ImagesApiBinding,Option<String>) '
                                                                                     '= $target;}',
                                                                      'owningTarget': 'pi_ai_providers::images::ImagesApiRegistry::register',
                                                                      'sourceKind': 'value',
                                                                      'sources': ['images-runtime'],
                                                                      'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-models.ts:CreateImagesProviderOptions': {'occurrences': [{'key': './compat:CreateImagesProviderOptions',
                                                                                   'routeFeatures': ['compat'],
                                                                                   'routePackage': 'pi-ai',
                                                                                   'routeProbe': 'fn '
                                                                                                 'probe(id:ImagesProviderId,a:ProviderAuth,g:Arc<dyn '
                                                                                                 'img::ImagesGenerator>,source:Arc<dyn '
                                                                                                 'img::ImagesModelsSource>,spawner:Arc<dyn '
                                                                                                 'TaskSpawner>){let '
                                                                                                 'mut '
                                                                                                 'v=$target{id,name:None,auth:a,models:Arc::from([]),api:g,refresh:None};v.refresh=Some(img::ImagesRefreshConfig{source,spawner});let '
                                                                                                 '_:Option<img::ImagesRefreshConfig>=v.refresh;}',
                                                                                   'routeTarget': 'pi_ai::compat::CreateImagesProviderOptions',
                                                                                   'sources': ['images-runtime'],
                                                                                   'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                             'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                             'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                             'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                             'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                             'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                             'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                             'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                             'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                             'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                             'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                             'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                             'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                             'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                                  {'key': '.:CreateImagesProviderOptions',
                                                                                   'routeFeatures': ['images-runtime'],
                                                                                   'routePackage': 'pi-ai',
                                                                                   'routeProbe': 'fn '
                                                                                                 'probe(id:ImagesProviderId,a:ProviderAuth,g:Arc<dyn '
                                                                                                 'img::ImagesGenerator>,source:Arc<dyn '
                                                                                                 'img::ImagesModelsSource>,spawner:Arc<dyn '
                                                                                                 'TaskSpawner>){let '
                                                                                                 'mut '
                                                                                                 'v=$target{id,name:None,auth:a,models:Arc::from([]),api:g,refresh:None};v.refresh=Some(img::ImagesRefreshConfig{source,spawner});let '
                                                                                                 '_:Option<img::ImagesRefreshConfig>=v.refresh;}',
                                                                                   'routeTarget': 'pi_ai::CreateImagesProviderOptions',
                                                                                   'sources': ['images-runtime'],
                                                                                   'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                             'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                             'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                             'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                             'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                             'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                             'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                             'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                             'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                             'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                             'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                                  'owningFeatures': [],
                                                                  'owningPackage': 'pi-ai-providers',
                                                                  'owningProbe': 'fn '
                                                                                 'probe(id:ImagesProviderId,a:ProviderAuth,g:Arc<dyn '
                                                                                 'img::ImagesGenerator>,source:Arc<dyn '
                                                                                 'img::ImagesModelsSource>,spawner:Arc<dyn '
                                                                                 'TaskSpawner>){let mut '
                                                                                 'v=$target{id,name:None,auth:a,models:Arc::from([]),api:g,refresh:None};v.refresh=Some(img::ImagesRefreshConfig{source,spawner});let '
                                                                                 '_:Option<img::ImagesRefreshConfig>=v.refresh;}',
                                                                  'owningTarget': 'pi_ai_providers::images::CreateImagesProviderOptions',
                                                                  'sourceKind': 'type',
                                                                  'sources': ['images-runtime'],
                                                                  'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                            'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                            'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                            'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                            'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                            'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                            'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                            'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                            'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                            'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                            'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-models.ts:ImagesModels': {'occurrences': [{'key': './compat:ImagesModels',
                                                                    'routeFeatures': ['compat'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'fn '
                                                                                  'probe(o:img::ImagesModelsOptions,id:&ImagesProviderId,m:&ModelId){let '
                                                                                  'v=$target::new(o);let '
                                                                                  '_:Vec<Arc<dyn '
                                                                                  'img::ImagesProvider>>=v.get_providers();let '
                                                                                  '_:Vec<ImagesModel>=v.get_models(Some(id));let '
                                                                                  '_:Option<ImagesModel>=v.get_model(id,m);let '
                                                                                  "_:BoxFuture<'_,Result<(),ModelsError>>=v.refresh(Some(id));}",
                                                                    'routeTarget': 'pi_ai::compat::ImagesModels',
                                                                    'sources': ['images-runtime'],
                                                                    'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                              'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                              'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                              'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                              'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                              'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                              'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                              'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                              'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                              'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                              'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                              'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                   {'key': '.:ImagesModels',
                                                                    'routeFeatures': ['images-runtime'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'fn '
                                                                                  'probe(o:img::ImagesModelsOptions,id:&ImagesProviderId,m:&ModelId){let '
                                                                                  'v=$target::new(o);let '
                                                                                  '_:Vec<Arc<dyn '
                                                                                  'img::ImagesProvider>>=v.get_providers();let '
                                                                                  '_:Vec<ImagesModel>=v.get_models(Some(id));let '
                                                                                  '_:Option<ImagesModel>=v.get_model(id,m);let '
                                                                                  "_:BoxFuture<'_,Result<(),ModelsError>>=v.refresh(Some(id));}",
                                                                    'routeTarget': 'pi_ai::ImagesModels',
                                                                    'sources': ['images-runtime'],
                                                                    'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                              'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                              'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                              'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                              'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                              'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                              'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                              'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                              'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                              'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                              'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                   'owningFeatures': [],
                                                   'owningPackage': 'pi-ai-providers',
                                                   'owningProbe': 'fn '
                                                                  'probe(o:img::ImagesModelsOptions,id:&ImagesProviderId,m:&ModelId){let '
                                                                  'v=$target::new(o);let _:Vec<Arc<dyn '
                                                                  'img::ImagesProvider>>=v.get_providers();let '
                                                                  '_:Vec<ImagesModel>=v.get_models(Some(id));let '
                                                                  '_:Option<ImagesModel>=v.get_model(id,m);let '
                                                                  "_:BoxFuture<'_,Result<(),ModelsError>>=v.refresh(Some(id));}",
                                                   'owningTarget': 'pi_ai_providers::images::ImagesModels',
                                                   'sourceKind': 'type',
                                                   'sources': ['images-runtime'],
                                                   'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                             'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                             'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                             'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                             'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                             'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                             'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                             'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                             'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                             'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                             'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-models.ts:ImagesProvider': {'occurrences': [{'key': './compat:ImagesProvider',
                                                                      'routeFeatures': ['compat'],
                                                                      'routePackage': 'pi-ai',
                                                                      'routeProbe': 'use $target as '
                                                                                    'Required; struct Probe{ '
                                                                                    'id:ImagesProviderId,auth:ProviderAuth '
                                                                                    '} impl Required for '
                                                                                    'Probe {fn '
                                                                                    'id(&self)->&ImagesProviderId{&self.id}fn '
                                                                                    'name(&self)->&str{"probe"}fn '
                                                                                    'auth(&self)->&ProviderAuth{&self.auth}fn '
                                                                                    'models(&self)->Result<Arc<[ImagesModel]>,ModelsError>{Ok(Arc::from([]))}fn '
                                                                                    'models_refresher(&self)->Option<&(dyn '
                                                                                    "img::ImagesModelsRefresher+'_ "
                                                                                    ')>{None}fn '
                                                                                    "generate<'a>(&'a "
                                                                                    "self,_:ImageGenerationRequest)->Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>{Ok(Box::pin(std::future::pending()))}} "
                                                                                    'fn probe(v:&Probe){let '
                                                                                    '_: &dyn Required=v;}',
                                                                      'routeTarget': 'pi_ai::compat::ImagesProvider',
                                                                      'sources': ['images-runtime'],
                                                                      'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                     {'key': '.:ImagesProvider',
                                                                      'routeFeatures': ['images-runtime'],
                                                                      'routePackage': 'pi-ai',
                                                                      'routeProbe': 'use $target as '
                                                                                    'Required; struct Probe{ '
                                                                                    'id:ImagesProviderId,auth:ProviderAuth '
                                                                                    '} impl Required for '
                                                                                    'Probe {fn '
                                                                                    'id(&self)->&ImagesProviderId{&self.id}fn '
                                                                                    'name(&self)->&str{"probe"}fn '
                                                                                    'auth(&self)->&ProviderAuth{&self.auth}fn '
                                                                                    'models(&self)->Result<Arc<[ImagesModel]>,ModelsError>{Ok(Arc::from([]))}fn '
                                                                                    'models_refresher(&self)->Option<&(dyn '
                                                                                    "img::ImagesModelsRefresher+'_ "
                                                                                    ')>{None}fn '
                                                                                    "generate<'a>(&'a "
                                                                                    "self,_:ImageGenerationRequest)->Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>{Ok(Box::pin(std::future::pending()))}} "
                                                                                    'fn probe(v:&Probe){let '
                                                                                    '_: &dyn Required=v;}',
                                                                      'routeTarget': 'pi_ai::ImagesProvider',
                                                                      'sources': ['images-runtime'],
                                                                      'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                     'owningFeatures': [],
                                                     'owningPackage': 'pi-ai-providers',
                                                     'owningProbe': 'use $target as Required; struct Probe{ '
                                                                    'id:ImagesProviderId,auth:ProviderAuth } '
                                                                    'impl Required for Probe {fn '
                                                                    'id(&self)->&ImagesProviderId{&self.id}fn '
                                                                    'name(&self)->&str{"probe"}fn '
                                                                    'auth(&self)->&ProviderAuth{&self.auth}fn '
                                                                    'models(&self)->Result<Arc<[ImagesModel]>,ModelsError>{Ok(Arc::from([]))}fn '
                                                                    'models_refresher(&self)->Option<&(dyn '
                                                                    "img::ImagesModelsRefresher+'_ "
                                                                    ")>{None}fn generate<'a>(&'a "
                                                                    "self,_:ImageGenerationRequest)->Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>{Ok(Box::pin(std::future::pending()))}} "
                                                                    'fn probe(v:&Probe){let _: &dyn '
                                                                    'Required=v;}',
                                                     'owningTarget': 'pi_ai_providers::images::ImagesProvider',
                                                     'sourceKind': 'type',
                                                     'sources': ['images-runtime'],
                                                     'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                               'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                               'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                               'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                               'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                               'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                               'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                               'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                               'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                               'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                               'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-models.ts:MutableImagesModels': {'occurrences': [{'key': './compat:MutableImagesModels',
                                                                           'routeFeatures': ['compat'],
                                                                           'routePackage': 'pi-ai',
                                                                           'routeProbe': 'fn '
                                                                                         'probe(value:img::ImagesModels){let '
                                                                                         '_: $target=value;}',
                                                                           'routeTarget': 'pi_ai::compat::MutableImagesModels',
                                                                           'sources': ['images-runtime'],
                                                                           'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                     'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                     'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                     'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                     'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                     'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                     'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                     'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                     'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                     'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                     'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                     'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                     'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                     'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                          {'key': '.:MutableImagesModels',
                                                                           'routeFeatures': ['images-runtime'],
                                                                           'routePackage': 'pi-ai',
                                                                           'routeProbe': 'fn '
                                                                                         'probe(value:img::ImagesModels){let '
                                                                                         '_: $target=value;}',
                                                                           'routeTarget': 'pi_ai::MutableImagesModels',
                                                                           'sources': ['images-runtime'],
                                                                           'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                     'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                     'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                     'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                     'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                     'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                     'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                     'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                     'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                     'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                     'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                          'owningFeatures': [],
                                                          'owningPackage': 'pi-ai-providers',
                                                          'owningProbe': 'fn '
                                                                         'probe(value:img::ImagesModels){let '
                                                                         '_: $target=value;}',
                                                          'owningTarget': 'pi_ai_providers::images::MutableImagesModels',
                                                          'sourceKind': 'type',
                                                          'sources': ['images-runtime'],
                                                          'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                    'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                    'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                    'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                    'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                    'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                    'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                    'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                    'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                    'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                    'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-models.ts:createImagesModels': {'occurrences': [{'key': './compat:createImagesModels',
                                                                          'routeFeatures': ['compat'],
                                                                          'routePackage': 'pi-ai',
                                                                          'routeProbe': 'fn probe(){let _: '
                                                                                        'fn(img::ImagesModelsOptions)->img::MutableImagesModels '
                                                                                        '= $target;}',
                                                                          'routeTarget': 'pi_ai::compat::create_images_models',
                                                                          'sources': ['images-runtime'],
                                                                          'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                    'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                    'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                    'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                    'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                    'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                    'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                    'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                    'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                    'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                    'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                    'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                         {'key': '.:createImagesModels',
                                                                          'routeFeatures': ['images-runtime'],
                                                                          'routePackage': 'pi-ai',
                                                                          'routeProbe': 'fn probe(){let _: '
                                                                                        'fn(img::ImagesModelsOptions)->img::MutableImagesModels '
                                                                                        '= $target;}',
                                                                          'routeTarget': 'pi_ai::create_images_models',
                                                                          'sources': ['images-runtime'],
                                                                          'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                    'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                    'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                    'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                    'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                    'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                    'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                    'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                    'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                    'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                    'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                         'owningFeatures': [],
                                                         'owningPackage': 'pi-ai-providers',
                                                         'owningProbe': 'fn probe(){let _: '
                                                                        'fn(img::ImagesModelsOptions)->img::MutableImagesModels '
                                                                        '= $target;}',
                                                         'owningTarget': 'pi_ai_providers::images::create_images_models',
                                                         'sourceKind': 'value',
                                                         'sources': ['images-runtime'],
                                                         'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                   'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                   'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                   'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                   'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                   'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                   'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                   'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                   'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                   'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                   'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images-models.ts:createImagesProvider': {'occurrences': [{'key': './compat:createImagesProvider',
                                                                            'routeFeatures': ['compat'],
                                                                            'routePackage': 'pi-ai',
                                                                            'routeProbe': 'fn probe(){let _: '
                                                                                          'fn(img::CreateImagesProviderOptions)->Arc<dyn '
                                                                                          'img::ImagesProvider> '
                                                                                          '= $target;}',
                                                                            'routeTarget': 'pi_ai::compat::create_images_provider',
                                                                            'sources': ['images-runtime'],
                                                                            'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                      'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                      'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                      'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                      'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                      'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                      'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                      'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                      'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                      'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                      'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                      'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                           {'key': '.:createImagesProvider',
                                                                            'routeFeatures': ['images-runtime'],
                                                                            'routePackage': 'pi-ai',
                                                                            'routeProbe': 'fn probe(){let _: '
                                                                                          'fn(img::CreateImagesProviderOptions)->Arc<dyn '
                                                                                          'img::ImagesProvider> '
                                                                                          '= $target;}',
                                                                            'routeTarget': 'pi_ai::create_images_provider',
                                                                            'sources': ['images-runtime'],
                                                                            'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                      'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                      'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                      'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                      'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                      'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                      'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                      'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                      'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                      'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                      'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']}],
                                                           'owningFeatures': [],
                                                           'owningPackage': 'pi-ai-providers',
                                                           'owningProbe': 'fn probe(){let _: '
                                                                          'fn(img::CreateImagesProviderOptions)->Arc<dyn '
                                                                          'img::ImagesProvider> = $target;}',
                                                           'owningTarget': 'pi_ai_providers::images::create_images_provider',
                                                           'sourceKind': 'value',
                                                           'sources': ['images-runtime'],
                                                           'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                     'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                     'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                     'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                     'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                     'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                     'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                     'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                     'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                     'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                     'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/images.ts:generateImages': {'occurrences': [{'key': './compat:generateImages',
                                                               'routeFeatures': ['compat'],
                                                               'routePackage': 'pi-ai',
                                                               'routeProbe': "fn probe(){let _: for<'a> "
                                                                             "fn(&'a "
                                                                             "img::ImagesApiRegistry,ImageGenerationRequest)->BoxFuture<'a,Result<AssistantImages,ModelsError>> "
                                                                             '= $target;}',
                                                               'routeTarget': 'pi_ai::compat::ImagesApiRegistry::generate',
                                                               'sources': ['images-runtime', 'facade'],
                                                               'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                         'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                         'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                         'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                         'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                         'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                         'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                         'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                         'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                         'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                         'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                              'owningFeatures': [],
                                              'owningPackage': 'pi-ai-providers',
                                              'owningProbe': "fn probe(){let _: for<'a> fn(&'a "
                                                             "img::ImagesApiRegistry,ImageGenerationRequest)->BoxFuture<'a,Result<AssistantImages,ModelsError>> "
                                                             '= $target;}',
                                              'owningTarget': 'pi_ai_providers::images::ImagesApiRegistry::generate',
                                              'sourceKind': 'value',
                                              'sources': ['images-runtime'],
                                              'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                        'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                        'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                        'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                        'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                        'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                        'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                        'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                        'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                        'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                        'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/legacy-api-aliases.ts:streamAnthropic': {'occurrences': [{'key': './compat:streamAnthropic',
                                                                            'routeFeatures': ['compat'],
                                                                            'routePackage': 'pi-ai',
                                                                            'routeProbe': 'fn probe(){let _: '
                                                                                          'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::anthropic_messages::AnthropicOptions)->AssistantStream '
                                                                                          '= $target;}',
                                                                            'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_anthropic',
                                                                            'sources': ['facade'],
                                                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                      'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                           'owningFeatures': ['compat'],
                                                           'owningPackage': 'pi-ai',
                                                           'owningProbe': 'fn probe(){let _: '
                                                                          'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::anthropic_messages::AnthropicOptions)->AssistantStream '
                                                                          '= $target;}',
                                                           'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_anthropic',
                                                           'sourceKind': 'value',
                                                           'sources': ['facade'],
                                                           'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                     'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                     'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamAzureOpenAIResponses': {'occurrences': [{'key': './compat:streamAzureOpenAIResponses',
                                                                                       'routeFeatures': ['compat'],
                                                                                       'routePackage': 'pi-ai',
                                                                                       'routeProbe': 'fn '
                                                                                                     'probe(){let '
                                                                                                     '_: '
                                                                                                     'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::azure_openai_responses::AzureOpenAIResponsesOptions)->AssistantStream '
                                                                                                     '= '
                                                                                                     '$target;}',
                                                                                       'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_azure_openai_responses',
                                                                                       'sources': ['facade'],
                                                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                                 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                      'owningFeatures': ['compat'],
                                                                      'owningPackage': 'pi-ai',
                                                                      'owningProbe': 'fn probe(){let _: '
                                                                                     'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::azure_openai_responses::AzureOpenAIResponsesOptions)->AssistantStream '
                                                                                     '= $target;}',
                                                                      'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_azure_openai_responses',
                                                                      'sourceKind': 'value',
                                                                      'sources': ['facade'],
                                                                      'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamGoogle': {'occurrences': [{'key': './compat:streamGoogle',
                                                                         'routeFeatures': ['compat'],
                                                                         'routePackage': 'pi-ai',
                                                                         'routeProbe': 'fn probe(){let _: '
                                                                                       'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::google_genai::GoogleOptions)->AssistantStream '
                                                                                       '= $target;}',
                                                                         'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_google',
                                                                         'sources': ['facade'],
                                                                         'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                   'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                        'owningFeatures': ['compat'],
                                                        'owningPackage': 'pi-ai',
                                                        'owningProbe': 'fn probe(){let _: '
                                                                       'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::google_genai::GoogleOptions)->AssistantStream '
                                                                       '= $target;}',
                                                        'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_google',
                                                        'sourceKind': 'value',
                                                        'sources': ['facade'],
                                                        'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamGoogleVertex': {'occurrences': [{'key': './compat:streamGoogleVertex',
                                                                               'routeFeatures': ['compat'],
                                                                               'routePackage': 'pi-ai',
                                                                               'routeProbe': 'fn probe(){let '
                                                                                             '_: '
                                                                                             'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::google_vertex::GoogleVertexOptions)->AssistantStream '
                                                                                             '= $target;}',
                                                                               'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_google_vertex',
                                                                               'sources': ['facade'],
                                                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                              'owningFeatures': ['compat'],
                                                              'owningPackage': 'pi-ai',
                                                              'owningProbe': 'fn probe(){let _: '
                                                                             'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::google_vertex::GoogleVertexOptions)->AssistantStream '
                                                                             '= $target;}',
                                                              'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_google_vertex',
                                                              'sourceKind': 'value',
                                                              'sources': ['facade'],
                                                              'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                        'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                        'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamMistral': {'occurrences': [{'key': './compat:streamMistral',
                                                                          'routeFeatures': ['compat'],
                                                                          'routePackage': 'pi-ai',
                                                                          'routeProbe': 'fn probe(){let _: '
                                                                                        'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::mistral_conversations::MistralOptions)->AssistantStream '
                                                                                        '= $target;}',
                                                                          'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_mistral',
                                                                          'sources': ['facade'],
                                                                          'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                    'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                         'owningFeatures': ['compat'],
                                                         'owningPackage': 'pi-ai',
                                                         'owningProbe': 'fn probe(){let _: '
                                                                        'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::mistral_conversations::MistralOptions)->AssistantStream '
                                                                        '= $target;}',
                                                         'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_mistral',
                                                         'sourceKind': 'value',
                                                         'sources': ['facade'],
                                                         'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamOpenAICodexResponses': {'occurrences': [{'key': './compat:streamOpenAICodexResponses',
                                                                                       'routeFeatures': ['compat'],
                                                                                       'routePackage': 'pi-ai',
                                                                                       'routeProbe': 'fn '
                                                                                                     'probe(){let '
                                                                                                     '_: '
                                                                                                     'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::openai_codex_responses::OpenAICodexResponsesOptions)->AssistantStream '
                                                                                                     '= '
                                                                                                     '$target;}',
                                                                                       'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_openai_codex_responses',
                                                                                       'sources': ['facade'],
                                                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                                 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                      'owningFeatures': ['compat'],
                                                                      'owningPackage': 'pi-ai',
                                                                      'owningProbe': 'fn probe(){let _: '
                                                                                     'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::openai_codex_responses::OpenAICodexResponsesOptions)->AssistantStream '
                                                                                     '= $target;}',
                                                                      'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_openai_codex_responses',
                                                                      'sourceKind': 'value',
                                                                      'sources': ['facade'],
                                                                      'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamOpenAICompletions': {'occurrences': [{'key': './compat:streamOpenAICompletions',
                                                                                    'routeFeatures': ['compat'],
                                                                                    'routePackage': 'pi-ai',
                                                                                    'routeProbe': 'fn '
                                                                                                  'probe(){let '
                                                                                                  '_: '
                                                                                                  'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::openai_completions::OpenAICompletionsOptions)->AssistantStream '
                                                                                                  '= '
                                                                                                  '$target;}',
                                                                                    'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_openai_completions',
                                                                                    'sources': ['facade'],
                                                                                    'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                              'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                   'owningFeatures': ['compat'],
                                                                   'owningPackage': 'pi-ai',
                                                                   'owningProbe': 'fn probe(){let _: '
                                                                                  'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::openai_completions::OpenAICompletionsOptions)->AssistantStream '
                                                                                  '= $target;}',
                                                                   'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_openai_completions',
                                                                   'sourceKind': 'value',
                                                                   'sources': ['facade'],
                                                                   'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                             'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                             'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamOpenAIResponses': {'occurrences': [{'key': './compat:streamOpenAIResponses',
                                                                                  'routeFeatures': ['compat'],
                                                                                  'routePackage': 'pi-ai',
                                                                                  'routeProbe': 'fn '
                                                                                                'probe(){let '
                                                                                                '_: '
                                                                                                'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::openai_responses::OpenAIResponsesOptions)->AssistantStream '
                                                                                                '= $target;}',
                                                                                  'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_openai_responses',
                                                                                  'sources': ['facade'],
                                                                                  'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                            'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                 'owningFeatures': ['compat'],
                                                                 'owningPackage': 'pi-ai',
                                                                 'owningProbe': 'fn probe(){let _: '
                                                                                'fn(&pi_ai::compat::LegacyApiAliases,TextStreamRequest,pi_ai_providers::api::openai_responses::OpenAIResponsesOptions)->AssistantStream '
                                                                                '= $target;}',
                                                                 'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_openai_responses',
                                                                 'sourceKind': 'value',
                                                                 'sources': ['facade'],
                                                                 'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                           'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                           'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleAnthropic': {'occurrences': [{'key': './compat:streamSimpleAnthropic',
                                                                                  'routeFeatures': ['compat'],
                                                                                  'routePackage': 'pi-ai',
                                                                                  'routeProbe': 'fn '
                                                                                                'probe(){let '
                                                                                                '_: '
                                                                                                'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                                '= $target;}',
                                                                                  'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_anthropic',
                                                                                  'sources': ['facade'],
                                                                                  'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                            'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                            'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                            'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                 'owningFeatures': ['compat'],
                                                                 'owningPackage': 'pi-ai',
                                                                 'owningProbe': 'fn probe(){let _: '
                                                                                'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                '= $target;}',
                                                                 'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_anthropic',
                                                                 'sourceKind': 'value',
                                                                 'sources': ['facade'],
                                                                 'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                           'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                           'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleAzureOpenAIResponses': {'occurrences': [{'key': './compat:streamSimpleAzureOpenAIResponses',
                                                                                             'routeFeatures': ['compat'],
                                                                                             'routePackage': 'pi-ai',
                                                                                             'routeProbe': 'fn '
                                                                                                           'probe(){let '
                                                                                                           '_: '
                                                                                                           'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                                           '= '
                                                                                                           '$target;}',
                                                                                             'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_azure_openai_responses',
                                                                                             'sources': ['facade'],
                                                                                             'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                                       'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                            'owningFeatures': ['compat'],
                                                                            'owningPackage': 'pi-ai',
                                                                            'owningProbe': 'fn probe(){let '
                                                                                           '_: '
                                                                                           'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                           '= $target;}',
                                                                            'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_azure_openai_responses',
                                                                            'sourceKind': 'value',
                                                                            'sources': ['facade'],
                                                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleGoogle': {'occurrences': [{'key': './compat:streamSimpleGoogle',
                                                                               'routeFeatures': ['compat'],
                                                                               'routePackage': 'pi-ai',
                                                                               'routeProbe': 'fn probe(){let '
                                                                                             '_: '
                                                                                             'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                             '= $target;}',
                                                                               'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_google',
                                                                               'sources': ['facade'],
                                                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                         'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                              'owningFeatures': ['compat'],
                                                              'owningPackage': 'pi-ai',
                                                              'owningProbe': 'fn probe(){let _: '
                                                                             'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                             '= $target;}',
                                                              'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_google',
                                                              'sourceKind': 'value',
                                                              'sources': ['facade'],
                                                              'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                        'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                        'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleGoogleVertex': {'occurrences': [{'key': './compat:streamSimpleGoogleVertex',
                                                                                     'routeFeatures': ['compat'],
                                                                                     'routePackage': 'pi-ai',
                                                                                     'routeProbe': 'fn '
                                                                                                   'probe(){let '
                                                                                                   '_: '
                                                                                                   'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                                   '= '
                                                                                                   '$target;}',
                                                                                     'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_google_vertex',
                                                                                     'sources': ['facade'],
                                                                                     'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                               'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                               'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                               'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                               'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                               'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                    'owningFeatures': ['compat'],
                                                                    'owningPackage': 'pi-ai',
                                                                    'owningProbe': 'fn probe(){let _: '
                                                                                   'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                   '= $target;}',
                                                                    'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_google_vertex',
                                                                    'sourceKind': 'value',
                                                                    'sources': ['facade'],
                                                                    'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                              'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                              'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleMistral': {'occurrences': [{'key': './compat:streamSimpleMistral',
                                                                                'routeFeatures': ['compat'],
                                                                                'routePackage': 'pi-ai',
                                                                                'routeProbe': 'fn '
                                                                                              'probe(){let '
                                                                                              '_: '
                                                                                              'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                              '= $target;}',
                                                                                'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_mistral',
                                                                                'sources': ['facade'],
                                                                                'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                          'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                          'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                          'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                               'owningFeatures': ['compat'],
                                                               'owningPackage': 'pi-ai',
                                                               'owningProbe': 'fn probe(){let _: '
                                                                              'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                              '= $target;}',
                                                               'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_mistral',
                                                               'sourceKind': 'value',
                                                               'sources': ['facade'],
                                                               'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                         'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                         'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleOpenAICodexResponses': {'occurrences': [{'key': './compat:streamSimpleOpenAICodexResponses',
                                                                                             'routeFeatures': ['compat'],
                                                                                             'routePackage': 'pi-ai',
                                                                                             'routeProbe': 'fn '
                                                                                                           'probe(){let '
                                                                                                           '_: '
                                                                                                           'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                                           '= '
                                                                                                           '$target;}',
                                                                                             'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_openai_codex_responses',
                                                                                             'sources': ['facade'],
                                                                                             'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                                       'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                       'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                            'owningFeatures': ['compat'],
                                                                            'owningPackage': 'pi-ai',
                                                                            'owningProbe': 'fn probe(){let '
                                                                                           '_: '
                                                                                           'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                           '= $target;}',
                                                                            'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_openai_codex_responses',
                                                                            'sourceKind': 'value',
                                                                            'sources': ['facade'],
                                                                            'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                      'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                      'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleOpenAICompletions': {'occurrences': [{'key': './compat:streamSimpleOpenAICompletions',
                                                                                          'routeFeatures': ['compat'],
                                                                                          'routePackage': 'pi-ai',
                                                                                          'routeProbe': 'fn '
                                                                                                        'probe(){let '
                                                                                                        '_: '
                                                                                                        'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                                        '= '
                                                                                                        '$target;}',
                                                                                          'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_openai_completions',
                                                                                          'sources': ['facade'],
                                                                                          'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                                    'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                    'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                    'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                         'owningFeatures': ['compat'],
                                                                         'owningPackage': 'pi-ai',
                                                                         'owningProbe': 'fn probe(){let _: '
                                                                                        'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                        '= $target;}',
                                                                         'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_openai_completions',
                                                                         'sourceKind': 'value',
                                                                         'sources': ['facade'],
                                                                         'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                   'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                   'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/legacy-api-aliases.ts:streamSimpleOpenAIResponses': {'occurrences': [{'key': './compat:streamSimpleOpenAIResponses',
                                                                                        'routeFeatures': ['compat'],
                                                                                        'routePackage': 'pi-ai',
                                                                                        'routeProbe': 'fn '
                                                                                                      'probe(){let '
                                                                                                      '_: '
                                                                                                      'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                                      '= '
                                                                                                      '$target;}',
                                                                                        'routeTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_openai_responses',
                                                                                        'sources': ['facade'],
                                                                                        'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat',
                                                                                                  'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                  'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                  'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                       'owningFeatures': ['compat'],
                                                                       'owningPackage': 'pi-ai',
                                                                       'owningProbe': 'fn probe(){let _: '
                                                                                      'fn(&pi_ai::compat::LegacyApiAliases,SimpleStreamRequest)->AssistantStream '
                                                                                      '= $target;}',
                                                                       'owningTarget': 'pi_ai::compat::LegacyApiAliases::stream_simple_openai_responses',
                                                                       'sourceKind': 'value',
                                                                       'sources': ['facade'],
                                                                       'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
 'packages/ai/src/providers/all.ts:BuiltinProvider': {'occurrences': [{'key': './compat:BuiltinProvider',
                                                                       'routeFeatures': ['compat'],
                                                                       'routePackage': 'pi-ai',
                                                                       'routeProbe': 'fn probe(){let '
                                                                                     'value=$target::OpenAI;let '
                                                                                     "_: &'static "
                                                                                     'str=value.as_str();}',
                                                                       'routeTarget': 'pi_ai::compat::BuiltinProvider',
                                                                       'sources': ['selected-factories'],
                                                                       'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                 'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers',
                                                                                 'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                 'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                 'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']},
                                                                      {'key': './providers/all:BuiltinProvider',
                                                                       'routeFeatures': ['catalog-openai'],
                                                                       'routePackage': 'pi-ai',
                                                                       'routeProbe': 'fn probe(){let '
                                                                                     'value=$target::OpenAI;let '
                                                                                     "_: &'static "
                                                                                     'str=value.as_str();}',
                                                                       'routeTarget': 'pi_ai::catalog::BuiltinProvider',
                                                                       'sources': ['selected-factories'],
                                                                       'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                 'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                      'owningFeatures': ['catalog-openai'],
                                                      'owningPackage': 'pi-ai',
                                                      'owningProbe': 'fn probe(){let '
                                                                     "value=$target::OpenAI;let _: &'static "
                                                                     'str=value.as_str();}',
                                                      'owningTarget': 'pi_ai::catalog::BuiltinProvider',
                                                      'sourceKind': 'type',
                                                      'sources': ['selected-factories'],
                                                      'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:builtinImagesModels': {'occurrences': [{'key': './providers/all:builtinImagesModels',
                                                                           'routeFeatures': ['images-openrouter'],
                                                                           'routePackage': 'pi-ai',
                                                                           'routeProbe': 'fn probe(){let _: '
                                                                                         'fn(&pi_ai::builtins::BuiltinProviderModules,img::ImagesModelsOptions)->Result<img::MutableImagesModels,pi_ai::builtins::ProviderFactoryError> '
                                                                                         '= $target;}',
                                                                           'routeTarget': 'pi_ai::builtins::builtin_images_models',
                                                                           'sources': ['selected-factories'],
                                                                           'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                     'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                          'owningFeatures': ['images-openrouter'],
                                                          'owningPackage': 'pi-ai',
                                                          'owningProbe': 'fn probe(){let _: '
                                                                         'fn(&pi_ai::builtins::BuiltinProviderModules,img::ImagesModelsOptions)->Result<img::MutableImagesModels,pi_ai::builtins::ProviderFactoryError> '
                                                                         '= $target;}',
                                                          'owningTarget': 'pi_ai::builtins::builtin_images_models',
                                                          'sourceKind': 'value',
                                                          'sources': ['selected-factories'],
                                                          'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                    'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:builtinImagesProviders': {'occurrences': [{'key': './providers/all:builtinImagesProviders',
                                                                              'routeFeatures': ['images-openrouter'],
                                                                              'routePackage': 'pi-ai',
                                                                              'routeProbe': 'fn probe(){let '
                                                                                            '_: '
                                                                                            'fn(&pi_ai::builtins::BuiltinProviderModules)->Result<Vec<Arc<dyn '
                                                                                            'img::ImagesProvider>>,pi_ai::builtins::ProviderFactoryError> '
                                                                                            '= $target;}',
                                                                              'routeTarget': 'pi_ai::builtins::builtin_images_providers',
                                                                              'sources': ['selected-factories'],
                                                                              'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                        'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                        'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                        'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                        'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                        'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                        'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                             'owningFeatures': ['images-openrouter'],
                                                             'owningPackage': 'pi-ai',
                                                             'owningProbe': 'fn probe(){let _: '
                                                                            'fn(&pi_ai::builtins::BuiltinProviderModules)->Result<Vec<Arc<dyn '
                                                                            'img::ImagesProvider>>,pi_ai::builtins::ProviderFactoryError> '
                                                                            '= $target;}',
                                                             'owningTarget': 'pi_ai::builtins::builtin_images_providers',
                                                             'sourceKind': 'value',
                                                             'sources': ['selected-factories'],
                                                             'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                       'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                       'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                       'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                       'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                       'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                       'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:builtinModels': {'occurrences': [{'key': './providers/all:builtinModels',
                                                                     'routeFeatures': ['all-providers'],
                                                                     'routePackage': 'pi-ai',
                                                                     'routeProbe': 'fn '
                                                                                   "output<'a,F:Future<Output=Result<ModelsRegistry,pi_ai::builtins::BuiltinCompositionError>>+Send+'a>(_:F){} "
                                                                                   "fn probe<'a>(m:&'a "
                                                                                   'pi_ai::builtins::BuiltinProviderModules,o:ModelsRegistryOptions){output($target(m,o));}',
                                                                     'routeTarget': 'pi_ai::builtins::builtin_models',
                                                                     'sources': ['selected-factories'],
                                                                     'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                               'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                    'owningFeatures': ['all-providers'],
                                                    'owningPackage': 'pi-ai',
                                                    'owningProbe': 'fn '
                                                                   "output<'a,F:Future<Output=Result<ModelsRegistry,pi_ai::builtins::BuiltinCompositionError>>+Send+'a>(_:F){} "
                                                                   "fn probe<'a>(m:&'a "
                                                                   'pi_ai::builtins::BuiltinProviderModules,o:ModelsRegistryOptions){output($target(m,o));}',
                                                    'owningTarget': 'pi_ai::builtins::builtin_models',
                                                    'sourceKind': 'value',
                                                    'sources': ['selected-factories'],
                                                    'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                              'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                              'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                              'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                              'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                              'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                              'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:builtinProviders': {'occurrences': [{'key': './providers/all:builtinProviders',
                                                                        'routeFeatures': ['all-providers'],
                                                                        'routePackage': 'pi-ai',
                                                                        'routeProbe': 'fn probe(){let _: '
                                                                                      'fn(&pi_ai::builtins::BuiltinProviderModules)->Result<Vec<Arc<dyn '
                                                                                      'Provider>>,pi_ai::builtins::ProviderFactoryError> '
                                                                                      '= $target;}',
                                                                        'routeTarget': 'pi_ai::builtins::builtin_providers',
                                                                        'sources': ['selected-factories'],
                                                                        'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                  'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                       'owningFeatures': ['all-providers'],
                                                       'owningPackage': 'pi-ai',
                                                       'owningProbe': 'fn probe(){let _: '
                                                                      'fn(&pi_ai::builtins::BuiltinProviderModules)->Result<Vec<Arc<dyn '
                                                                      'Provider>>,pi_ai::builtins::ProviderFactoryError> '
                                                                      '= $target;}',
                                                       'owningTarget': 'pi_ai::builtins::builtin_providers',
                                                       'sourceKind': 'value',
                                                       'sources': ['selected-factories'],
                                                       'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                 'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:getBuiltinModel': {'occurrences': [{'key': './providers/all:getBuiltinModel',
                                                                       'routeFeatures': ['catalog-openai'],
                                                                       'routePackage': 'pi-ai',
                                                                       'routeProbe': 'fn probe(){let _: '
                                                                                     'fn(&str,&str)->Result<Option<Model>,pi_ai::catalog::CatalogError> '
                                                                                     '= $target;}',
                                                                       'routeTarget': 'pi_ai::catalog::get_builtin_model',
                                                                       'sources': ['selected-factories'],
                                                                       'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                 'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                 'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                      'owningFeatures': ['catalog-openai'],
                                                      'owningPackage': 'pi-ai',
                                                      'owningProbe': 'fn probe(){let _: '
                                                                     'fn(&str,&str)->Result<Option<Model>,pi_ai::catalog::CatalogError> '
                                                                     '= $target;}',
                                                      'owningTarget': 'pi_ai::catalog::get_builtin_model',
                                                      'sourceKind': 'value',
                                                      'sources': ['selected-factories'],
                                                      'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:getBuiltinModelDataGeneratedAt': {'occurrences': [{'key': './providers/all:getBuiltinModelDataGeneratedAt',
                                                                                      'routeFeatures': ['catalog-openai'],
                                                                                      'routePackage': 'pi-ai',
                                                                                      'routeProbe': 'fn '
                                                                                                    'probe(){let '
                                                                                                    '_: '
                                                                                                    'fn()->Option<SafeUnsigned> '
                                                                                                    '= '
                                                                                                    '$target;}',
                                                                                      'routeTarget': 'pi_ai::catalog::get_builtin_model_data_generated_at',
                                                                                      'sources': ['selected-factories'],
                                                                                      'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                                'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                                'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                                'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                                'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                                'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                                'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                                     'owningFeatures': ['catalog-openai'],
                                                                     'owningPackage': 'pi-ai',
                                                                     'owningProbe': 'fn probe(){let _: '
                                                                                    'fn()->Option<SafeUnsigned> '
                                                                                    '= $target;}',
                                                                     'owningTarget': 'pi_ai::catalog::get_builtin_model_data_generated_at',
                                                                     'sourceKind': 'value',
                                                                     'sources': ['selected-factories'],
                                                                     'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                               'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                               'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:getBuiltinModels': {'occurrences': [{'key': './providers/all:getBuiltinModels',
                                                                        'routeFeatures': ['catalog-openai'],
                                                                        'routePackage': 'pi-ai',
                                                                        'routeProbe': 'fn probe(){let _: '
                                                                                      'fn(&str)->Result<Vec<Model>,pi_ai::catalog::CatalogError> '
                                                                                      '= $target;}',
                                                                        'routeTarget': 'pi_ai::catalog::get_builtin_models',
                                                                        'sources': ['selected-factories'],
                                                                        'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                  'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                  'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                       'owningFeatures': ['catalog-openai'],
                                                       'owningPackage': 'pi-ai',
                                                       'owningProbe': 'fn probe(){let _: '
                                                                      'fn(&str)->Result<Vec<Model>,pi_ai::catalog::CatalogError> '
                                                                      '= $target;}',
                                                       'owningTarget': 'pi_ai::catalog::get_builtin_models',
                                                       'sourceKind': 'value',
                                                       'sources': ['selected-factories'],
                                                       'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                 'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                 'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/all.ts:getBuiltinProviders': {'occurrences': [{'key': './providers/all:getBuiltinProviders',
                                                                           'routeFeatures': ['catalog-openai'],
                                                                           'routePackage': 'pi-ai',
                                                                           'routeProbe': 'fn probe(){let _: '
                                                                                         'fn()->Vec<pi_ai::catalog::BuiltinProvider> '
                                                                                         '= $target;}',
                                                                           'routeTarget': 'pi_ai::catalog::get_builtin_providers',
                                                                           'sources': ['selected-factories'],
                                                                           'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                     'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                     'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                          'owningFeatures': ['catalog-openai'],
                                                          'owningPackage': 'pi-ai',
                                                          'owningProbe': 'fn probe(){let _: '
                                                                         'fn()->Vec<pi_ai::catalog::BuiltinProvider> '
                                                                         '= $target;}',
                                                          'owningTarget': 'pi_ai::catalog::get_builtin_providers',
                                                          'sourceKind': 'value',
                                                          'sources': ['selected-factories'],
                                                          'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                    'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                    'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/anthropic.models.ts:ANTHROPIC_MODELS': {'occurrences': [{'key': './providers/anthropic.models:ANTHROPIC_MODELS',
                                                                                     'routeFeatures': ['catalog-anthropic'],
                                                                                     'routePackage': 'pi-ai-providers',
                                                                                     'routeProbe': 'fn '
                                                                                                   'probe(){let '
                                                                                                   '_: '
                                                                                                   'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                                   '= '
                                                                                                   '$target;}',
                                                                                     'routeTarget': 'pi_ai_providers::catalog::anthropic_models',
                                                                                     'sources': ['catalog-generator'],
                                                                                     'tests': ['pi-ai-providers/catalog#captured_anthropic_matches_complete_source@catalog-anthropic']}],
                                                                    'owningFeatures': ['catalog-anthropic'],
                                                                    'owningPackage': 'pi-ai-providers',
                                                                    'owningProbe': 'fn probe(){let _: '
                                                                                   'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                   '= $target;}',
                                                                    'owningTarget': 'pi_ai_providers::catalog::anthropic_models',
                                                                    'sourceKind': 'value',
                                                                    'sources': ['catalog-generator'],
                                                                    'tests': ['pi-ai-providers/catalog#captured_anthropic_matches_complete_source@catalog-anthropic']},
 'packages/ai/src/providers/anthropic.ts:anthropicProvider': {'occurrences': [{'key': './providers/anthropic:anthropicProvider',
                                                                               'routeFeatures': ['provider-anthropic'],
                                                                               'routePackage': 'pi-ai-providers',
                                                                               'routeProbe': 'fn probe(){let '
                                                                                             '_: '
                                                                                             'fn(Arc<pi_ai_providers::api::anthropic_messages::AnthropicMessagesApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                                             'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                             '= $target;}',
                                                                               'routeTarget': 'pi_ai_providers::providers::anthropic_provider',
                                                                               'sources': ['selected-factories'],
                                                                               'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                         'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                         'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                         'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                         'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                         'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                         'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                              'owningFeatures': ['provider-anthropic'],
                                                              'owningPackage': 'pi-ai-providers',
                                                              'owningProbe': 'fn probe(){let _: '
                                                                             'fn(Arc<pi_ai_providers::api::anthropic_messages::AnthropicMessagesApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                             'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                             '= $target;}',
                                                              'owningTarget': 'pi_ai_providers::providers::anthropic_provider',
                                                              'sourceKind': 'value',
                                                              'sources': ['selected-factories'],
                                                              'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                        'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                        'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                        'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                        'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                        'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                        'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/deepseek.models.ts:DEEPSEEK_MODELS': {'occurrences': [{'key': './providers/deepseek.models:DEEPSEEK_MODELS',
                                                                                   'routeFeatures': ['catalog-deepseek'],
                                                                                   'routePackage': 'pi-ai-providers',
                                                                                   'routeProbe': 'fn '
                                                                                                 'probe(){let '
                                                                                                 '_: '
                                                                                                 'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                                 '= '
                                                                                                 '$target;}',
                                                                                   'routeTarget': 'pi_ai_providers::catalog::deepseek_models',
                                                                                   'sources': ['catalog-generator'],
                                                                                   'tests': ['pi-ai-providers/catalog#captured_deepseek_matches_complete_source@catalog-deepseek']}],
                                                                  'owningFeatures': ['catalog-deepseek'],
                                                                  'owningPackage': 'pi-ai-providers',
                                                                  'owningProbe': 'fn probe(){let _: '
                                                                                 'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                 '= $target;}',
                                                                  'owningTarget': 'pi_ai_providers::catalog::deepseek_models',
                                                                  'sourceKind': 'value',
                                                                  'sources': ['catalog-generator'],
                                                                  'tests': ['pi-ai-providers/catalog#captured_deepseek_matches_complete_source@catalog-deepseek']},
 'packages/ai/src/providers/deepseek.ts:deepseekProvider': {'occurrences': [{'key': './providers/deepseek:deepseekProvider',
                                                                             'routeFeatures': ['provider-deepseek'],
                                                                             'routePackage': 'pi-ai-providers',
                                                                             'routeProbe': 'fn probe(){let '
                                                                                           '_: '
                                                                                           'fn(Arc<pi_ai_providers::api::openai_completions::OpenAICompletionsApi>)->Result<Arc<dyn '
                                                                                           'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                           '= $target;}',
                                                                             'routeTarget': 'pi_ai_providers::providers::deepseek_provider',
                                                                             'sources': ['selected-factories'],
                                                                             'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                       'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                            'owningFeatures': ['provider-deepseek'],
                                                            'owningPackage': 'pi-ai-providers',
                                                            'owningProbe': 'fn probe(){let _: '
                                                                           'fn(Arc<pi_ai_providers::api::openai_completions::OpenAICompletionsApi>)->Result<Arc<dyn '
                                                                           'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                           '= $target;}',
                                                            'owningTarget': 'pi_ai_providers::providers::deepseek_provider',
                                                            'sourceKind': 'value',
                                                            'sources': ['selected-factories'],
                                                            'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                      'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                      'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                      'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                      'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                      'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                      'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/google.models.ts:GOOGLE_MODELS': {'occurrences': [{'key': './providers/google.models:GOOGLE_MODELS',
                                                                               'routeFeatures': ['catalog-google'],
                                                                               'routePackage': 'pi-ai-providers',
                                                                               'routeProbe': 'fn probe(){let '
                                                                                             '_: '
                                                                                             'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                             '= $target;}',
                                                                               'routeTarget': 'pi_ai_providers::catalog::google_models',
                                                                               'sources': ['catalog-generator'],
                                                                               'tests': ['pi-ai-providers/catalog#captured_google_matches_complete_source@catalog-google']}],
                                                              'owningFeatures': ['catalog-google'],
                                                              'owningPackage': 'pi-ai-providers',
                                                              'owningProbe': 'fn probe(){let _: '
                                                                             'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                             '= $target;}',
                                                              'owningTarget': 'pi_ai_providers::catalog::google_models',
                                                              'sourceKind': 'value',
                                                              'sources': ['catalog-generator'],
                                                              'tests': ['pi-ai-providers/catalog#captured_google_matches_complete_source@catalog-google']},
 'packages/ai/src/providers/google.ts:googleProvider': {'occurrences': [{'key': './providers/google:googleProvider',
                                                                         'routeFeatures': ['provider-google'],
                                                                         'routePackage': 'pi-ai-providers',
                                                                         'routeProbe': 'fn probe(){let _: '
                                                                                       'fn(Arc<pi_ai_providers::api::google_genai::GoogleGenAiApi>)->Result<Arc<dyn '
                                                                                       'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                       '= $target;}',
                                                                         'routeTarget': 'pi_ai_providers::providers::google_provider',
                                                                         'sources': ['selected-factories'],
                                                                         'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                   'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                        'owningFeatures': ['provider-google'],
                                                        'owningPackage': 'pi-ai-providers',
                                                        'owningProbe': 'fn probe(){let _: '
                                                                       'fn(Arc<pi_ai_providers::api::google_genai::GoogleGenAiApi>)->Result<Arc<dyn '
                                                                       'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                       '= $target;}',
                                                        'owningTarget': 'pi_ai_providers::providers::google_provider',
                                                        'sourceKind': 'value',
                                                        'sources': ['selected-factories'],
                                                        'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                  'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/images/register-builtins.ts:generateImagesOpenRouter': {'occurrences': [{'key': './compat:generateImagesOpenRouter',
                                                                                                     'routeFeatures': ['compat'],
                                                                                                     'routePackage': 'pi-ai',
                                                                                                     'routeProbe': 'fn '
                                                                                                                   'probe(){let '
                                                                                                                   '_: '
                                                                                                                   "for<'a> "
                                                                                                                   "fn(&'a "
                                                                                                                   "pi_ai::compat::CompatFacade,ImageGenerationRequest)->img::ImagesGenerateCall<'a> "
                                                                                                                   '= '
                                                                                                                   '$target;}',
                                                                                                     'routeTarget': 'pi_ai::compat::CompatFacade::generate_images_openrouter',
                                                                                                     'sources': ['images-runtime',
                                                                                                                 'facade'],
                                                                                                     'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                                               'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                                               'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                                               'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                                               'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                                               'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                                               'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                                               'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                                               'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                                               'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                                               'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                                               'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                               'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                               'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                                    'owningFeatures': [],
                                                                                    'owningPackage': 'pi-ai-providers',
                                                                                    'owningProbe': 'fn '
                                                                                                   'probe(){let '
                                                                                                   '_: '
                                                                                                   "for<'a> "
                                                                                                   "fn(&'a "
                                                                                                   "img::CachedImagesApi,ImageGenerationRequest)->img::ImagesGenerateCall<'a> "
                                                                                                   '= '
                                                                                                   '$target;}',
                                                                                    'owningTarget': '<pi_ai_providers::images::CachedImagesApi '
                                                                                                    'as '
                                                                                                    'pi_ai_providers::images::ImagesGenerator>::generate',
                                                                                    'sourceKind': 'value',
                                                                                    'sources': ['images-runtime'],
                                                                                    'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                              'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                              'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                              'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                              'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                              'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                              'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                              'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                              'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                              'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                              'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/providers/images/register-builtins.ts:registerBuiltInImagesApiProviders': {'occurrences': [{'key': './compat:registerBuiltInImagesApiProviders',
                                                                                                              'routeFeatures': ['compat'],
                                                                                                              'routePackage': 'pi-ai',
                                                                                                              'routeProbe': 'fn '
                                                                                                                            'probe(){let '
                                                                                                                            '_: '
                                                                                                                            'fn(&pi_ai::compat::CompatFacade) '
                                                                                                                            '= '
                                                                                                                            '$target;}',
                                                                                                              'routeTarget': 'pi_ai::compat::CompatFacade::register_builtin_images_api_providers',
                                                                                                              'sources': ['images-runtime',
                                                                                                                          'facade'],
                                                                                                              'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                                                        'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                                                        'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                                                        'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                                                        'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                                                        'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                                                        'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                                                        'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                                                        'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                                                        'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                                                        'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@',
                                                                                                                        'pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                                                                                                        'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                                                                                                        'pi-ai/compat_facade#actual_root_compat_and_oauth_projection_routes_are_callable@compat']}],
                                                                                             'owningFeatures': ['compat'],
                                                                                             'owningPackage': 'pi-ai',
                                                                                             'owningProbe': 'fn '
                                                                                                            'probe(){let '
                                                                                                            '_: '
                                                                                                            'fn(&pi_ai::compat::CompatFacade) '
                                                                                                            '= '
                                                                                                            '$target;}',
                                                                                             'owningTarget': 'pi_ai::compat::CompatFacade::register_builtin_images_api_providers',
                                                                                             'sourceKind': 'value',
                                                                                             'sources': ['images-runtime'],
                                                                                             'tests': ['pi-ai-providers/images_runtime#complete_source_generation_cases_preserve_auth_branch_rejection_and_exact_merge@',
                                                                                                       'pi-ai-providers/images_runtime#source_ordered_membership_best_effort_lists_and_first_match_remain_instance_owned@',
                                                                                                       'pi-ai-providers/images_runtime#shared_refresh_retains_old_models_on_failure_retries_and_outlives_dropped_waiters@',
                                                                                                       'pi-ai-providers/images_runtime#all_provider_refresh_admits_every_future_and_settles_failures_best_effort@',
                                                                                                       'pi-ai-providers/images_runtime#explicit_registry_replacement_retains_old_binding_checks_and_missing_api_rejects@',
                                                                                                       'pi-ai-providers/images_runtime#source_synchronous_refresh_throw_stays_memoized_while_async_rejection_retries@',
                                                                                                       'pi-ai-providers/images_runtime#complete_source_live_listing_and_refresh_admission_preserve_bounded_membership_reentry@',
                                                                                                       'pi-ai-providers/images_runtime#complete_source_nested_refresh_launch_and_completion_clear_current_flight_without_fencing@',
                                                                                                       'pi-ai-providers/images_runtime#complete_source_lazy_modes_preserve_per_call_loading_and_cached_success_or_failure@',
                                                                                                       'pi-ai-providers/images_runtime#cached_load_survives_caller_drop_and_shared_failure_releases_all_waiters@',
                                                                                                       'pi-ai-providers/images_runtime#configured_loader_retains_one_adapter_and_dropped_waiter_waker_can_reenter@']},
 'packages/ai/src/providers/openai-codex.models.ts:OPENAI_CODEX_MODELS': {'occurrences': [{'key': './providers/openai-codex.models:OPENAI_CODEX_MODELS',
                                                                                           'routeFeatures': ['catalog-openai-codex'],
                                                                                           'routePackage': 'pi-ai-providers',
                                                                                           'routeProbe': 'fn '
                                                                                                         'probe(){let '
                                                                                                         '_: '
                                                                                                         'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                                         '= '
                                                                                                         '$target;}',
                                                                                           'routeTarget': 'pi_ai_providers::catalog::openai_codex_models',
                                                                                           'sources': ['catalog-generator'],
                                                                                           'tests': ['pi-ai-providers/catalog#captured_codex_matches_complete_source@catalog-openai-codex']}],
                                                                          'owningFeatures': ['catalog-openai-codex'],
                                                                          'owningPackage': 'pi-ai-providers',
                                                                          'owningProbe': 'fn probe(){let _: '
                                                                                         'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                         '= $target;}',
                                                                          'owningTarget': 'pi_ai_providers::catalog::openai_codex_models',
                                                                          'sourceKind': 'value',
                                                                          'sources': ['catalog-generator'],
                                                                          'tests': ['pi-ai-providers/catalog#captured_codex_matches_complete_source@catalog-openai-codex']},
 'packages/ai/src/providers/openai-codex.ts:openaiCodexProvider': {'occurrences': [{'key': './providers/openai-codex:openaiCodexProvider',
                                                                                    'routeFeatures': ['provider-openai-codex'],
                                                                                    'routePackage': 'pi-ai-providers',
                                                                                    'routeProbe': 'fn '
                                                                                                  'probe(){let '
                                                                                                  '_: '
                                                                                                  'fn(Arc<pi_ai_providers::api::openai_codex_responses::OpenAICodexResponsesApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                                                  'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                                  '= '
                                                                                                  '$target;}',
                                                                                    'routeTarget': 'pi_ai_providers::providers::openai_codex_provider',
                                                                                    'sources': ['selected-factories'],
                                                                                    'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                              'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                              'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                              'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                              'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                              'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                              'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                                   'owningFeatures': ['provider-openai-codex'],
                                                                   'owningPackage': 'pi-ai-providers',
                                                                   'owningProbe': 'fn probe(){let _: '
                                                                                  'fn(Arc<pi_ai_providers::api::openai_codex_responses::OpenAICodexResponsesApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                                  'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                  '= $target;}',
                                                                   'owningTarget': 'pi_ai_providers::providers::openai_codex_provider',
                                                                   'sourceKind': 'value',
                                                                   'sources': ['selected-factories'],
                                                                   'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                             'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                             'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                             'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                             'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                             'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                             'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/openai.models.ts:OPENAI_MODELS': {'occurrences': [{'key': './providers/openai.models:OPENAI_MODELS',
                                                                               'routeFeatures': ['catalog-openai'],
                                                                               'routePackage': 'pi-ai-providers',
                                                                               'routeProbe': 'fn probe(){let '
                                                                                             '_: '
                                                                                             'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                             '= $target;}',
                                                                               'routeTarget': 'pi_ai_providers::catalog::openai_models',
                                                                               'sources': ['catalog-generator'],
                                                                               'tests': ['pi-ai-providers/catalog#captured_openai_matches_complete_source@catalog-openai']}],
                                                              'owningFeatures': ['catalog-openai'],
                                                              'owningPackage': 'pi-ai-providers',
                                                              'owningProbe': 'fn probe(){let _: '
                                                                             'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                             '= $target;}',
                                                              'owningTarget': 'pi_ai_providers::catalog::openai_models',
                                                              'sourceKind': 'value',
                                                              'sources': ['catalog-generator'],
                                                              'tests': ['pi-ai-providers/catalog#captured_openai_matches_complete_source@catalog-openai']},
 'packages/ai/src/providers/openai.ts:openaiProvider': {'occurrences': [{'key': './providers/openai:openaiProvider',
                                                                         'routeFeatures': ['provider-openai'],
                                                                         'routePackage': 'pi-ai-providers',
                                                                         'routeProbe': 'fn probe(){let _: '
                                                                                       'fn(Arc<pi_ai_providers::api::openai_responses::OpenAIResponsesApi>)->Result<Arc<dyn '
                                                                                       'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                       '= $target;}',
                                                                         'routeTarget': 'pi_ai_providers::providers::openai_provider',
                                                                         'sources': ['selected-factories'],
                                                                         'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                   'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                   'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                        'owningFeatures': ['provider-openai'],
                                                        'owningPackage': 'pi-ai-providers',
                                                        'owningProbe': 'fn probe(){let _: '
                                                                       'fn(Arc<pi_ai_providers::api::openai_responses::OpenAIResponsesApi>)->Result<Arc<dyn '
                                                                       'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                       '= $target;}',
                                                        'owningTarget': 'pi_ai_providers::providers::openai_provider',
                                                        'sourceKind': 'value',
                                                        'sources': ['selected-factories'],
                                                        'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                  'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                  'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/openrouter-images.ts:openrouterImagesProvider': {'occurrences': [{'key': './providers/openrouter-images:openrouterImagesProvider',
                                                                                              'routeFeatures': ['images-openrouter'],
                                                                                              'routePackage': 'pi-ai-providers',
                                                                                              'routeProbe': 'fn '
                                                                                                            'probe(){let '
                                                                                                            '_: '
                                                                                                            'fn(Arc<img::openrouter::OpenRouterImagesApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                                                            'img::ImagesProvider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                                            '= '
                                                                                                            '$target;}',
                                                                                              'routeTarget': 'pi_ai_providers::providers::openrouter_images_provider',
                                                                                              'sources': ['selected-factories'],
                                                                                              'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                                        'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                                        'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                                        'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                                        'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                                        'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                                        'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                                             'owningFeatures': ['images-openrouter'],
                                                                             'owningPackage': 'pi-ai-providers',
                                                                             'owningProbe': 'fn probe(){let '
                                                                                            '_: '
                                                                                            'fn(Arc<img::openrouter::OpenRouterImagesApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                                            'img::ImagesProvider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                            '= $target;}',
                                                                             'owningTarget': 'pi_ai_providers::providers::openrouter_images_provider',
                                                                             'sourceKind': 'value',
                                                                             'sources': ['selected-factories'],
                                                                             'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                       'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                       'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/providers/openrouter.models.ts:OPENROUTER_MODELS': {'occurrences': [{'key': './providers/openrouter.models:OPENROUTER_MODELS',
                                                                                       'routeFeatures': ['catalog-openrouter'],
                                                                                       'routePackage': 'pi-ai-providers',
                                                                                       'routeProbe': 'fn '
                                                                                                     'probe(){let '
                                                                                                     '_: '
                                                                                                     'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                                     '= '
                                                                                                     '$target;}',
                                                                                       'routeTarget': 'pi_ai_providers::catalog::openrouter_models',
                                                                                       'sources': ['catalog-generator'],
                                                                                       'tests': ['pi-ai-providers/catalog#captured_openrouter_matches_complete_source@catalog-openrouter']}],
                                                                      'owningFeatures': ['catalog-openrouter'],
                                                                      'owningPackage': 'pi-ai-providers',
                                                                      'owningProbe': 'fn probe(){let _: '
                                                                                     'fn()->Result<Vec<Model>,pi_ai_providers::catalog::CatalogError> '
                                                                                     '= $target;}',
                                                                      'owningTarget': 'pi_ai_providers::catalog::openrouter_models',
                                                                      'sourceKind': 'value',
                                                                      'sources': ['catalog-generator'],
                                                                      'tests': ['pi-ai-providers/catalog#captured_openrouter_matches_complete_source@catalog-openrouter']},
 'packages/ai/src/providers/openrouter.ts:openrouterProvider': {'occurrences': [{'key': './providers/openrouter:openrouterProvider',
                                                                                 'routeFeatures': ['provider-openrouter'],
                                                                                 'routePackage': 'pi-ai-providers',
                                                                                 'routeProbe': 'fn '
                                                                                               'probe(){let '
                                                                                               '_: '
                                                                                               'fn(Arc<pi_ai_providers::api::openai_completions::OpenAICompletionsApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                                               'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                                               '= $target;}',
                                                                                 'routeTarget': 'pi_ai_providers::providers::openrouter_provider',
                                                                                 'sources': ['selected-factories'],
                                                                                 'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                                           'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                                           'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                                           'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                                           'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                                           'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                                           'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']}],
                                                                'owningFeatures': ['provider-openrouter'],
                                                                'owningPackage': 'pi-ai-providers',
                                                                'owningProbe': 'fn probe(){let _: '
                                                                               'fn(Arc<pi_ai_providers::api::openai_completions::OpenAICompletionsApi>,&pi_ai_providers::providers::FactoryAuthServices)->Result<Arc<dyn '
                                                                               'Provider>,pi_ai_providers::providers::ProviderFactoryError> '
                                                                               '= $target;}',
                                                                'owningTarget': 'pi_ai_providers::providers::openrouter_provider',
                                                                'sourceKind': 'value',
                                                                'sources': ['selected-factories'],
                                                                'tests': ['pi-ai-providers/selected_factories#source_metadata_catalog_order_static_capabilities_and_passive_modules@all-images,all-oauth,all-providers',
                                                                          'pi-ai-providers/selected_factories#awaited_builtin_registration_and_each_selected_factory_reaches_its_wire@all-images,all-oauth,all-providers',
                                                                          'pi-ai-providers/selected_factories#builtin_child_cancellation_retains_usage17_after_real_completions_handoff@all-images,all-oauth,all-providers',
                                                                          'pi-ai-providers/selected_factories#selected_lazy_capabilities_and_optional_oauth_loads_remain_separate@all-images,all-oauth,all-providers',
                                                                          'pi-ai-providers/selected_factories#facade_awaits_registration_failure_before_returning_a_registry@all-images,all-oauth,all-providers',
                                                                          'pi-ai-providers/selected_factories_source#complete_pinned_selected_factory_source_and_capture_are_fresh@',
                                                                          'pi-ai-providers/selected_factories_source#actual_linked_facade_assembly_and_partial_profiles_execute@all-images,all-providers']},
 'packages/ai/src/utils/retry.ts:RetryCallbacks': {'occurrences': [{'key': './compat:RetryCallbacks',
                                                                    'routeFeatures': ['compat'],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'use $target as '
                                                                                  'Required;struct '
                                                                                  'Probe;impl Required for '
                                                                                  'Probe{fn '
                                                                                  "on_retry_scheduled<'a>(&'a "
                                                                                  "self,_:SafeUnsigned,_:SafeUnsigned,_:pi_ai::RetryDelay,_:&'a "
                                                                                  "str)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}fn "
                                                                                  "on_retry_attempt_start<'a>(&'a "
                                                                                  "self)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}fn "
                                                                                  "on_retry_finished<'a>(&'a "
                                                                                  "self,_:bool,_:SafeUnsigned,_:Option<&'a "
                                                                                  "str>)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}}fn "
                                                                                  'probe(p:&Probe){let _: '
                                                                                  '&dyn Required=p;}',
                                                                    'routeTarget': 'pi_ai::compat::RetryCallbacks',
                                                                    'sources': ['retry'],
                                                                    'tests': ['pi-ai/retry#retry_loop_policy_absent@',
                                                                              'pi-ai/retry#retry_loop_policy_disabled@',
                                                                              'pi-ai/retry#retry_loop_zero_retries@',
                                                                              'pi-ai/retry#retry_loop_immediate_success@',
                                                                              'pi-ai/retry#retry_loop_immediate_aborted@',
                                                                              'pi-ai/retry#retry_loop_immediate_nonretryable@',
                                                                              'pi-ai/retry#retry_loop_exhaustion_call_count@',
                                                                              'pi-ai/retry#retry_loop_later_success@',
                                                                              'pi-ai/retry#retry_loop_aborted_after_scheduled@',
                                                                              'pi-ai/retry#retry_loop_observer_order@',
                                                                              'pi-ai/retry#retry_loop_exponential_delays@',
                                                                              'pi-ai/retry#retry_loop_cancel_during_sleep@',
                                                                              'pi-ai/retry#retry_loop_finish_only_after_retry@',
                                                                              'pi-ai/retry#retry_loop_finish_outcomes@',
                                                                              'pi-ai/retry#retry_rejected_operations_preserve_source_stage@',
                                                                              'pi-ai/retry#retry_cancellation_belongs_to_sleep@',
                                                                              'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@',
                                                                              'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@']},
                                                                   {'key': '.:RetryCallbacks',
                                                                    'routeFeatures': [],
                                                                    'routePackage': 'pi-ai',
                                                                    'routeProbe': 'use $target as '
                                                                                  'Required;struct '
                                                                                  'Probe;impl Required for '
                                                                                  'Probe{fn '
                                                                                  "on_retry_scheduled<'a>(&'a "
                                                                                  "self,_:SafeUnsigned,_:SafeUnsigned,_:pi_ai::RetryDelay,_:&'a "
                                                                                  "str)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}fn "
                                                                                  "on_retry_attempt_start<'a>(&'a "
                                                                                  "self)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}fn "
                                                                                  "on_retry_finished<'a>(&'a "
                                                                                  "self,_:bool,_:SafeUnsigned,_:Option<&'a "
                                                                                  "str>)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}}fn "
                                                                                  'probe(p:&Probe){let _: '
                                                                                  '&dyn Required=p;}',
                                                                    'routeTarget': 'pi_ai::RetryCallbacks',
                                                                    'sources': ['retry'],
                                                                    'tests': ['pi-ai/retry#retry_loop_policy_absent@',
                                                                              'pi-ai/retry#retry_loop_policy_disabled@',
                                                                              'pi-ai/retry#retry_loop_zero_retries@',
                                                                              'pi-ai/retry#retry_loop_immediate_success@',
                                                                              'pi-ai/retry#retry_loop_immediate_aborted@',
                                                                              'pi-ai/retry#retry_loop_immediate_nonretryable@',
                                                                              'pi-ai/retry#retry_loop_exhaustion_call_count@',
                                                                              'pi-ai/retry#retry_loop_later_success@',
                                                                              'pi-ai/retry#retry_loop_aborted_after_scheduled@',
                                                                              'pi-ai/retry#retry_loop_observer_order@',
                                                                              'pi-ai/retry#retry_loop_exponential_delays@',
                                                                              'pi-ai/retry#retry_loop_cancel_during_sleep@',
                                                                              'pi-ai/retry#retry_loop_finish_only_after_retry@',
                                                                              'pi-ai/retry#retry_loop_finish_outcomes@',
                                                                              'pi-ai/retry#retry_rejected_operations_preserve_source_stage@',
                                                                              'pi-ai/retry#retry_cancellation_belongs_to_sleep@',
                                                                              'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@',
                                                                              'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@']}],
                                                   'owningFeatures': [],
                                                   'owningPackage': 'pi-ai',
                                                   'owningProbe': 'use $target as Required;struct Probe;impl '
                                                                  'Required for Probe{fn '
                                                                  "on_retry_scheduled<'a>(&'a "
                                                                  "self,_:SafeUnsigned,_:SafeUnsigned,_:pi_ai::RetryDelay,_:&'a "
                                                                  "str)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}fn "
                                                                  "on_retry_attempt_start<'a>(&'a "
                                                                  "self)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}fn "
                                                                  "on_retry_finished<'a>(&'a "
                                                                  "self,_:bool,_:SafeUnsigned,_:Option<&'a "
                                                                  "str>)->BoxFuture<'a,Result<(),pi_ai::AssistantRetryError>>{Box::pin(std::future::pending())}}fn "
                                                                  'probe(p:&Probe){let _: &dyn Required=p;}',
                                                   'owningTarget': 'pi_ai::RetryCallbacks',
                                                   'sourceKind': 'type',
                                                   'sources': ['retry'],
                                                   'tests': ['pi-ai/retry#retry_loop_policy_absent@',
                                                             'pi-ai/retry#retry_loop_policy_disabled@',
                                                             'pi-ai/retry#retry_loop_zero_retries@',
                                                             'pi-ai/retry#retry_loop_immediate_success@',
                                                             'pi-ai/retry#retry_loop_immediate_aborted@',
                                                             'pi-ai/retry#retry_loop_immediate_nonretryable@',
                                                             'pi-ai/retry#retry_loop_exhaustion_call_count@',
                                                             'pi-ai/retry#retry_loop_later_success@',
                                                             'pi-ai/retry#retry_loop_aborted_after_scheduled@',
                                                             'pi-ai/retry#retry_loop_observer_order@',
                                                             'pi-ai/retry#retry_loop_exponential_delays@',
                                                             'pi-ai/retry#retry_loop_cancel_during_sleep@',
                                                             'pi-ai/retry#retry_loop_finish_only_after_retry@',
                                                             'pi-ai/retry#retry_loop_finish_outcomes@',
                                                             'pi-ai/retry#retry_rejected_operations_preserve_source_stage@',
                                                             'pi-ai/retry#retry_cancellation_belongs_to_sleep@',
                                                             'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@',
                                                             'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@']},
 'packages/ai/src/utils/retry.ts:retryAssistantCall': {'occurrences': [{'key': './compat:retryAssistantCall',
                                                                        'routeFeatures': ['compat'],
                                                                        'routePackage': 'pi-ai',
                                                                        'routeProbe': 'fn '
                                                                                      "output<'a,F:Future<Output=Result<AssistantMessage,pi_ai::AssistantRetryError>>+Send+'a>(_:F){} "
                                                                                      "fn probe<'a>(p:&'a "
                                                                                      'dyn '
                                                                                      "pi_ai::AssistantRetryProducer,o:Option<&'a "
                                                                                      "RetryPolicy>,c:&'a "
                                                                                      "CancellationToken,s:&'a "
                                                                                      'dyn '
                                                                                      "pi_ai::RetrySleeper,b:Option<&'a "
                                                                                      'dyn '
                                                                                      'pi_ai::RetryCallbacks>){output($target(p,o,c,s,b));}',
                                                                        'routeTarget': 'pi_ai::compat::retry_assistant_call',
                                                                        'sources': ['retry'],
                                                                        'tests': ['pi-ai/retry#retry_loop_policy_absent@',
                                                                                  'pi-ai/retry#retry_loop_policy_disabled@',
                                                                                  'pi-ai/retry#retry_loop_zero_retries@',
                                                                                  'pi-ai/retry#retry_loop_immediate_success@',
                                                                                  'pi-ai/retry#retry_loop_immediate_aborted@',
                                                                                  'pi-ai/retry#retry_loop_immediate_nonretryable@',
                                                                                  'pi-ai/retry#retry_loop_exhaustion_call_count@',
                                                                                  'pi-ai/retry#retry_loop_later_success@',
                                                                                  'pi-ai/retry#retry_loop_aborted_after_scheduled@',
                                                                                  'pi-ai/retry#retry_loop_observer_order@',
                                                                                  'pi-ai/retry#retry_loop_exponential_delays@',
                                                                                  'pi-ai/retry#retry_loop_cancel_during_sleep@',
                                                                                  'pi-ai/retry#retry_loop_finish_only_after_retry@',
                                                                                  'pi-ai/retry#retry_loop_finish_outcomes@',
                                                                                  'pi-ai/retry#retry_rejected_operations_preserve_source_stage@',
                                                                                  'pi-ai/retry#retry_cancellation_belongs_to_sleep@',
                                                                                  'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@',
                                                                                  'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@']},
                                                                       {'key': '.:retryAssistantCall',
                                                                        'routeFeatures': [],
                                                                        'routePackage': 'pi-ai',
                                                                        'routeProbe': 'fn '
                                                                                      "output<'a,F:Future<Output=Result<AssistantMessage,pi_ai::AssistantRetryError>>+Send+'a>(_:F){} "
                                                                                      "fn probe<'a>(p:&'a "
                                                                                      'dyn '
                                                                                      "pi_ai::AssistantRetryProducer,o:Option<&'a "
                                                                                      "RetryPolicy>,c:&'a "
                                                                                      "CancellationToken,s:&'a "
                                                                                      'dyn '
                                                                                      "pi_ai::RetrySleeper,b:Option<&'a "
                                                                                      'dyn '
                                                                                      'pi_ai::RetryCallbacks>){output($target(p,o,c,s,b));}',
                                                                        'routeTarget': 'pi_ai::retry_assistant_call',
                                                                        'sources': ['retry'],
                                                                        'tests': ['pi-ai/retry#retry_loop_policy_absent@',
                                                                                  'pi-ai/retry#retry_loop_policy_disabled@',
                                                                                  'pi-ai/retry#retry_loop_zero_retries@',
                                                                                  'pi-ai/retry#retry_loop_immediate_success@',
                                                                                  'pi-ai/retry#retry_loop_immediate_aborted@',
                                                                                  'pi-ai/retry#retry_loop_immediate_nonretryable@',
                                                                                  'pi-ai/retry#retry_loop_exhaustion_call_count@',
                                                                                  'pi-ai/retry#retry_loop_later_success@',
                                                                                  'pi-ai/retry#retry_loop_aborted_after_scheduled@',
                                                                                  'pi-ai/retry#retry_loop_observer_order@',
                                                                                  'pi-ai/retry#retry_loop_exponential_delays@',
                                                                                  'pi-ai/retry#retry_loop_cancel_during_sleep@',
                                                                                  'pi-ai/retry#retry_loop_finish_only_after_retry@',
                                                                                  'pi-ai/retry#retry_loop_finish_outcomes@',
                                                                                  'pi-ai/retry#retry_rejected_operations_preserve_source_stage@',
                                                                                  'pi-ai/retry#retry_cancellation_belongs_to_sleep@',
                                                                                  'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@',
                                                                                  'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@']}],
                                                       'owningFeatures': [],
                                                       'owningPackage': 'pi-ai',
                                                       'owningProbe': 'fn '
                                                                      "output<'a,F:Future<Output=Result<AssistantMessage,pi_ai::AssistantRetryError>>+Send+'a>(_:F){} "
                                                                      "fn probe<'a>(p:&'a dyn "
                                                                      "pi_ai::AssistantRetryProducer,o:Option<&'a "
                                                                      "RetryPolicy>,c:&'a "
                                                                      "CancellationToken,s:&'a dyn "
                                                                      "pi_ai::RetrySleeper,b:Option<&'a dyn "
                                                                      'pi_ai::RetryCallbacks>){output($target(p,o,c,s,b));}',
                                                       'owningTarget': 'pi_ai::retry_assistant_call',
                                                       'sourceKind': 'value',
                                                       'sources': ['retry'],
                                                       'tests': ['pi-ai/retry#retry_loop_policy_absent@',
                                                                 'pi-ai/retry#retry_loop_policy_disabled@',
                                                                 'pi-ai/retry#retry_loop_zero_retries@',
                                                                 'pi-ai/retry#retry_loop_immediate_success@',
                                                                 'pi-ai/retry#retry_loop_immediate_aborted@',
                                                                 'pi-ai/retry#retry_loop_immediate_nonretryable@',
                                                                 'pi-ai/retry#retry_loop_exhaustion_call_count@',
                                                                 'pi-ai/retry#retry_loop_later_success@',
                                                                 'pi-ai/retry#retry_loop_aborted_after_scheduled@',
                                                                 'pi-ai/retry#retry_loop_observer_order@',
                                                                 'pi-ai/retry#retry_loop_exponential_delays@',
                                                                 'pi-ai/retry#retry_loop_cancel_during_sleep@',
                                                                 'pi-ai/retry#retry_loop_finish_only_after_retry@',
                                                                 'pi-ai/retry#retry_loop_finish_outcomes@',
                                                                 'pi-ai/retry#retry_rejected_operations_preserve_source_stage@',
                                                                 'pi-ai/retry#retry_cancellation_belongs_to_sleep@',
                                                                 'pi-ai/retry#retry_extreme_numeric_observations_and_clock_projection@',
                                                                 'pi-ai/retry#retry_complete_pinned_source_and_timer_fingerprint@']}}

SOURCE_POLICY = {'auth-foundation': {'args': [],
                     'beforeScript': [],
                     'executable': 'node',
                     'expected': 'conformance/provider-traces/auth-foundation/source-observations.json',
                     'mode': 'json',
                     'origins': ['packages/ai/src/auth/helpers.ts', 'packages/ai/src/auth/context.ts'],
                     'relation': 'complete-runtime',
                     'script': 'conformance/provider-traces/auth-foundation/source.mjs',
                     'scriptSha256': '2cd4d25cb341f60f20acec19905559c1590c3225b483c1616d86c742a58b4ea5'},
 'catalog-generator': {'args': ['--check'],
                       'beforeScript': [],
                       'executable': 'python3',
                       'expected': 'conformance/catalog/generated/provenance.json',
                       'mode': 'check',
                       'origins': ['packages/ai/src/providers/anthropic.models.ts',
                                   'packages/ai/src/providers/deepseek.models.ts',
                                   'packages/ai/src/providers/google.models.ts',
                                   'packages/ai/src/providers/openai.models.ts',
                                   'packages/ai/src/providers/openai-codex.models.ts',
                                   'packages/ai/src/providers/openrouter.models.ts',
                                   'packages/ai/scripts/generate-models.ts',
                                   'packages/ai/scripts/generate-image-models.ts'],
                       'relation': 'dated-generator-to-selected-shard-provenance',
                       'script': 'scripts/generate_selected_catalogs.py',
                       'scriptSha256': 'c7bd8d222095710a4cf9b7ac2fdd5a0f7cbe62016eb2bfaca36f4e99247883b2',
                       'stdout': 'verified selected catalog: 450 text models; captured and historical image '
                                 'provenance kept separate\n'},
 'codex-cache': {'args': ['--check'],
                 'beforeScript': [],
                 'executable': 'node',
                 'expected': 'conformance/provider-traces/openai-codex-responses/cache.json',
                 'mode': 'check',
                 'origins': ['packages/ai/src/api/openai-codex-responses.ts'],
                 'relation': 'complete-runtime',
                 'script': 'conformance/provider-traces/openai-codex-responses/cache-oracle.mjs',
                 'scriptSha256': 'b78fb463a37e40c83db5e9c08c0211ce1ca0c2639a08a335d39746315b1a7ae4',
                 'stdout': ''},
 'facade': {'args': ['--check'],
            'beforeScript': ['--experimental-vm-modules'],
            'executable': 'node',
            'expected': 'conformance/ai-compat/facade/source.json',
            'mode': 'check',
            'origins': ['packages/ai/src/compat.ts',
                        'packages/ai/src/legacy-api-aliases.ts',
                        'packages/ai/src/bedrock-provider.ts',
                        'packages/ai/src/images.ts',
                        'packages/ai/src/images-api-registry.ts',
                        'packages/ai/src/providers/images/register-builtins.ts',
                        'packages/ai/src/image-models.ts',
                        'packages/ai/src/session-resources.ts'],
            'relation': 'complete-runtime',
            'script': 'conformance/ai-compat/facade/source.mjs',
            'scriptSha256': 'b343c2a6c35a3e93256893ad638ae53d3d4fe2f4af83627d7936806543ac5ae1',
            'stdout': 'FACADE_COMPLETE_SOURCE_PASS: '
                      'binding/order/direct/key/16aliases/images/resource-scope\n'},
 'image-wire': {'args': ['--check'],
                'beforeScript': [],
                'executable': 'node',
                'expected': 'conformance/provider-traces/openrouter-images/trace.json',
                'mode': 'check',
                'origins': ['packages/ai/src/api/openrouter-images.ts'],
                'relation': 'complete-runtime',
                'script': 'conformance/provider-traces/openrouter-images/source.mjs',
                'scriptSha256': '12e85a06395cdfcf9e140a4ff827864a9aa987bd26b01a0c30f3eca4698c02a9',
                'stdout': 'OpenRouter image source: 49 cases verified\n'},
 'images-runtime': {'args': ['--check'],
                    'beforeScript': [],
                    'executable': 'node',
                    'expected': 'conformance/provider-traces/images-runtime/trace.json',
                    'mode': 'check',
                    'origins': ['packages/ai/src/images-models.ts',
                                'packages/ai/src/images-api-registry.ts',
                                'packages/ai/src/images.ts',
                                'packages/ai/src/api/openrouter-images.lazy.ts',
                                'packages/ai/src/providers/images/register-builtins.ts'],
                    'relation': 'complete-runtime',
                    'script': 'conformance/provider-traces/images-runtime/source.mjs',
                    'scriptSha256': 'd516568277ff95f480600db10c086d8e7863f67578e493bc60b39d21837d9265',
                    'stdout': 'Images runtime complete source verified: 16 generation cases plus '
                              'listing/refresh/auth/registry/lazy branches\n'},
 'legacy-env': {'args': [],
                'beforeScript': [],
                'executable': 'node',
                'expected': 'conformance/ai-domain/s05-export-closure/legacy-env-observations.json',
                'mode': 'json',
                'origins': ['packages/ai/src/env-api-keys.ts', 'packages/ai/src/utils/provider-env.ts'],
                'relation': 'complete-runtime',
                'script': 'conformance/ai-domain/s05-export-closure/source-legacy-env.mjs',
                'scriptSha256': '6a8fba563b33dd0c233e73472dc69da20ec5f8cdc34f8367cddff529bfdf210a'},
 'oauth-cli': {'args': [],
               'beforeScript': [],
               'executable': 'node',
               'expected': 'conformance/provider-traces/oauth-cli/observations.json',
               'mode': 'json',
               'origins': ['packages/ai/src/cli.ts'],
               'relation': 'complete-runtime',
               'script': 'conformance/provider-traces/oauth-cli/source.mjs',
               'scriptSha256': '889a8d9bde3d4478de3d196a7794861653ac633deceb19ce2d2020e508d086e7'},
 'oauth-types': {'args': ['--check'],
                 'beforeScript': [],
                 'executable': 'python3',
                 'expected': 'conformance/ai-compat/oauth-types/source-shape.json',
                 'mode': 'check',
                 'origins': ['packages/ai/src/compat/extension-oauth-types.ts'],
                 'relation': 'complete-declarations-and-export-alias',
                 'script': 'conformance/ai-compat/oauth-types/source.py',
                 'scriptSha256': 'b9adb75c57c38c8fa5f6a2ab101f855973061a13263496f92c84009ec679a8d5',
                 'stdout': 'seven symbols: complete pinned declaration and export proof matched\n'},
 'retry': {'args': [],
           'beforeScript': [],
           'executable': 'node',
           'expected': 'conformance/ai-compat/retry/source-observations.json',
           'mode': 'json',
           'origins': ['packages/ai/src/utils/retry.ts'],
           'relation': 'complete-runtime',
           'script': 'conformance/ai-compat/retry/source.mjs',
           'scriptSha256': '84c4dd4dbfef1d81d13f747099300c137799ca4c8df0e54b911eb1c282e9a31d'},
 'selected-factories': {'args': ['--check'],
                        'beforeScript': [],
                        'executable': 'node',
                        'expected': 'conformance/providers/selected/trace.json',
                        'mode': 'check',
                        'origins': ['packages/ai/src/providers/all.ts',
                                    'packages/ai/src/providers/anthropic.ts',
                                    'packages/ai/src/providers/deepseek.ts',
                                    'packages/ai/src/providers/google.ts',
                                    'packages/ai/src/providers/openai.ts',
                                    'packages/ai/src/providers/openai-codex.ts',
                                    'packages/ai/src/providers/openrouter.ts',
                                    'packages/ai/src/providers/openrouter-images.ts'],
                        'relation': 'complete-runtime',
                        'script': 'conformance/providers/selected/source.mjs',
                        'scriptSha256': 'd3f55dfa20f27c3c851fd4a29d47829b8f2c85df619a0c2402e3e7405a6396ac',
                        'stdout': 'selected factories: seven complete bodies; all.ts; actual '
                                  'models/lazy/auth helpers; source order/passive/cache controls passed\n'},
 'selected-oauth': {'args': [],
                    'beforeScript': [],
                    'executable': 'node',
                    'expected': 'conformance/provider-traces/oauth-selected/source-observations.json',
                    'mode': 'json',
                    'origins': ['packages/ai/src/auth/oauth/anthropic.ts',
                                'packages/ai/src/auth/oauth/openai-codex.ts',
                                'packages/ai/src/auth/oauth/openrouter.ts',
                                'packages/ai/src/auth/oauth/device-code.ts',
                                'packages/ai/src/auth/oauth/pkce.ts',
                                'packages/ai/src/auth/oauth/oauth-page.ts'],
                    'relation': 'complete-runtime',
                    'script': 'conformance/provider-traces/oauth-selected/source.mjs',
                    'scriptSha256': 'ec6a679ee7627fb100e7a50350b2b4cece2c4108f85e2ab85339c05cd6be76c8'}}

COMPOSITION_POLICY = {'catalog-developer-binaries': {'binaries': [{'name': 'catalog_gen',
                                              'path': 'src/bin/catalog_gen.rs',
                                              'requiredFeatures': ['dev-catalog-tools']},
                                             {'name': 'image_catalog_gen',
                                              'path': 'src/bin/image_catalog_gen.rs',
                                              'requiredFeatures': ['dev-catalog-tools']}],
                                'sources': ['catalog-generator'],
                                'tests': ['pi-ai/catalog_binaries#actual_binaries_help_default_check_and_exact_deterministic_family_outputs@dev-catalog-tools',
                                          'pi-ai/catalog_binaries#actual_binaries_reject_unknown_arguments_and_missing_workspace_without_output@dev-catalog-tools',
                                          'pi-ai/catalog_binaries#actual_binaries_reject_changed_capture_source_and_canonical_output_after_generation@dev-catalog-tools']},
 'codex-cleanup-ownership': {'nestedTests': ['exact_registration_remembered_override_shortcut_and_binding_snapshot',
                                             'captured_before_pending_env_explicit_bytes_marker_and_missing_api',
                                             'all_sixteen_aliases_reach_real_families_with_typed_raw_options',
                                             'image_upsert_cached_module_and_no_env_preserve_distinct_registry',
                                             'cleanup_tickets_preserve_each_failure_task_drop_and_two_explicit_scopes',
                                             'queued_task_lifetime_survives_scope_and_observer_drop',
                                             'bedrock_module_retains_accepted_override_and_fallback',
                                             'registry_host_identity_and_displaced_destructor_reenter_outside_lock',
                                             'close_ticket_completion_wakes_reentrant_observer_outside_lock',
                                             'native_close::actual_facade_busy_close_and_new_same_session_own_distinct_physical_snapshots',
                                             'native_close::never_polled_facade_close_drop_aborts_retired_native_socket'],
                             'sources': ['facade', 'codex-cache'],
                             'tests': ['pi-ai/compat_facade#complete_source_facade_and_discriminating_registry_controls@compat',
                                       'pi-ai/compat_facade#actual_linked_facade_executes_nine_non_native_contract_tests@compat',
                                       'pi-ai/compat_facade#actual_linked_facade_executes_physical_codex_snapshot_and_drop_proof@compat,native']},
 'oauth-developer-cli': {'binaries': [{'name': 'pi_ai_cli',
                                       'path': 'src/bin/pi_ai_cli.rs',
                                       'requiredFeatures': ['dev-oauth-cli']}],
                         'sources': ['oauth-cli', 'selected-oauth'],
                         'tests': ['pi-ai/oauth_cli#oauth_cli_complete_pinned_source_reexecution@dev-oauth-cli',
                                   'pi-ai/oauth_cli#oauth_cli_32_source_command_observations_and_explicit_normalizations@dev-oauth-cli',
                                   'pi-ai/oauth_cli#oauth_cli_auth_file_admission_and_preservation_controls@dev-oauth-cli',
                                   'pi-ai/oauth_cli#oauth_cli_decimal_prefix_and_prompt_variance@dev-oauth-cli',
                                   'pi-ai/oauth_cli#oauth_cli_real_binary_no_network_help_errors_pipes_and_eof@dev-oauth-cli',
                                   'pi-ai/oauth_cli#oauth_cli_reader_cancellation_identity_close_empty_and_eof@dev-oauth-cli',
                                   'pi-ai-providers/oauth_cli_host#oauth_cli_actual_selected_four_login_paths_and_export@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                   'pi-ai-providers/oauth_cli_host#oauth_cli_included_source_hashes_bind_host_proof@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                   'pi-ai-providers/oauth_cli_host#oauth_cli_browser_success_exits_while_physical_stdin_remains_blocked@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter']},
 'selected-oauth-flows': {'sources': ['selected-oauth'],
                          'tests': ['pi-ai-providers/oauth_selected#oauth_selected_complete_source_trace_parity@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#oauth_selected_full_pinned_source_reexecutes_with_exact_hashes@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#oauth_selected_source_credential_values_match_without_logging_tokens@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#oauth_openrouter_successful_body_read_failure_is_json_admission@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#oauth_openrouter_callback_timeout_preserves_fixed_page_detail@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_callback_reply_and_cleanup_stages@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_cancellation_releases_listener_and_prompt@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_codex_bind_fallback_and_anthropic_failure@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter',
                                    'pi-ai-providers/oauth_selected#native_composition::oauth_selected_native_stalled_primary_ack_retains_cancellation_and_timeout_owners@auth-native,oauth-anthropic,oauth-openai-codex,oauth-openrouter']}}

EVIDENCE_POLICY = {'conformance/ai-compat/facade/run.py': '39fc7af4bc79e0790917db8d0da78f8fd8eec2902593c4b5a1bb9db524770dea',
 'conformance/ai-compat/facade/runtime.rs': 'b8e914ccd1d1be3defd229cde28b10aaed9a8f0da2a6e996c14212ef2998e18a',
 'conformance/ai-compat/facade/source.json': 'c30769d90f3c034d02f8035597bbb1cdfa55ea44114201651233a3c93a53cf1b',
 'conformance/ai-compat/facade/source.mjs': 'b343c2a6c35a3e93256893ad638ae53d3d4fe2f4af83627d7936806543ac5ae1',
 'conformance/ai-compat/oauth-types/README.md': '29d7bb2dff18aa8b5fa88fb7fa5e57715c3bde9c179b99a0f78ff9e9db07deda',
 'conformance/ai-compat/oauth-types/source-shape.json': 'e6bbbb89eece72d8b936b32aa0fecedc9e60bed24ba4460baa349e926ccb6416',
 'conformance/ai-compat/oauth-types/source.py': 'b9adb75c57c38c8fa5f6a2ab101f855973061a13263496f92c84009ec679a8d5',
 'conformance/ai-compat/retry/README.md': 'f88c75ab3d14a54f67382b747cb4b57a8f14849d13f7752cde4ba3ce387ef5b0',
 'conformance/ai-compat/retry/cases.json': 'e76fd03b2bd30379b6075e9f6a20d4413c2309eff38cfd11aba603a9c657aae6',
 'conformance/ai-compat/retry/causal.mjs': '382a886d6922f85b312ecf64e74182cba64b306af495c988f9b17e588ac5ccfb',
 'conformance/ai-compat/retry/source-observations.json': '5f6bfe1c5ad04a4321e2ed491af7d40283a9d57cac23516e04478d9a3f496021',
 'conformance/ai-compat/retry/source.mjs': '84c4dd4dbfef1d81d13f747099300c137799ca4c8df0e54b911eb1c282e9a31d',
 'conformance/ai-domain/s05-export-closure/legacy-env-observations.json': '3aadf3fd636d2f96da4ff0ef74341ab0907682cbd946c95844c170557c3b9815',
 'conformance/ai-domain/s05-export-closure/source-legacy-env.mjs': '6a8fba563b33dd0c233e73472dc69da20ec5f8cdc34f8367cddff529bfdf210a',
 'conformance/catalog/README.md': 'e8d33e175e8d5788767da581845428f42699100eb47b7582eb53eb94acb16212',
 'conformance/catalog/capture.json': '0898cbffc0d5befc26b4580894872954faa355661be1097484b430dd96da0475',
 'conformance/catalog/developer-tools.md': 'c8682d272a58c6a129c5911c27ec2e1dde52509f40656e9935208835310319b7',
 'conformance/catalog/generated/captured-image.ts': 'f3a70168ca69729cff00523359d97f1780a9e1767fd7c38a6134b397de0658b2',
 'conformance/catalog/generated/historical-images.json': '47eda2365d546d347b4e96d8276cac8014ec92f5b4247b0036f0a0ea04a0cdd3',
 'conformance/catalog/generated/images/openrouter.json': '5c825ff85e7ca93476ee116ad9b83fd8c94aac05b1a68f775bf814d6e0ffd3fb',
 'conformance/catalog/generated/provenance.json': '8501b26d58d96754510f4d67d5e3f6bd5ffc7bf14c405cac79247ac5f2141ef8',
 'conformance/catalog/generated/source-data-manifest.json': '6b47bcbf3cb3aa439e47ca9b7dfec10a7c11a1b8b5efd7bce2e89a1dc746275c',
 'conformance/catalog/generated/source-grouped.json': 'b614ae580356dc6786980dce2a5dad44971c5d22978a19b85fc37bb9cf086959',
 'conformance/catalog/generated/source-images.json': '0f61c704da068b2f1e8e75106d2c4f3782825cceafd999f0033f09c2f8ba0ba3',
 'conformance/catalog/generated/source-text.json': '6169c2a428f8975c029c6992054e76f874881e5e2f8de382686ac0f4bacc1db9',
 'conformance/catalog/generated/text/anthropic.json': 'f6518198e54281c66d3161a4a016e0e2aafd2b265533d578c33bfc6208046c63',
 'conformance/catalog/generated/text/deepseek.json': 'bc6ed5a64cd973b5326d8bb2b7eef4c1545a7d240e2f63d2ad6fad2a7e2b7f7e',
 'conformance/catalog/generated/text/google.json': 'd306f43fe99af6870ec7449265b4ed61012baa0684843be81daa35a7465eca5a',
 'conformance/catalog/generated/text/openai-codex.json': '3c0f7a8f8121ad45fc3ce8660cbbaae7fcf4deea1d9b16f032b4018fe3ce937f',
 'conformance/catalog/generated/text/openai.json': '2062ed870cb0b26d383c5d23d9e16a73a166c3950be935862f9ec81a67844b82',
 'conformance/catalog/generated/text/openrouter.json': 'db683dd66d8d0ccdffda3976db161c1d589f2fc1256e0b1fac41d74d11db8a54',
 'conformance/catalog/inputs/models-dev.json': '9dfacfc96ca34b8593df150ded00470905261b5ba2bdc5bc15e9b32aa51ca16c',
 'conformance/catalog/inputs/nvidia.json': 'e3a0d721e1380401636796aa1a1dfd9af6df27147e6167e781da2bb33fcc90a4',
 'conformance/catalog/inputs/openrouter-images.json': 'fd3856028acf2a1d9e95825efd317555ed3e38378e2f0a5feb2cf15a89c5091b',
 'conformance/catalog/inputs/openrouter-text.json': '2af154ed8aabd00696dfdc646006a7b47608998917cbad83176ac2fcd0f51cb3',
 'conformance/catalog/inputs/vercel.json': 'be9033458e0c1ed4d01c6881754532c1d7ddb071dcb1cc61fd074f53ef679279',
 'conformance/catalog/offline-preload.mjs': '9ffe1a4ddaae04921fb367ee9fcd6561dd5c25056ea0b614391557926dd518f0',
 'conformance/catalog/project.mjs': '99369bb1219a47090d71556a1fa193d3d89598b899521b080ed0af426fbfcc7e',
 'conformance/catalog/source-manifest.json': '657fdb1217ed342982a63e7e77039a28df793378583fcb35aa39f5bb528d23f8',
 'conformance/provider-traces/auth-foundation/README.md': '38e90b038e2e0a4a46c0b229507205b58d8970f4dcf045146eb1e99e6c43011d',
 'conformance/provider-traces/auth-foundation/source-observations.json': '7af62f36379791b1e50c1b22ad95d50b99d4959b2c12931197e842ccf7274a2d',
 'conformance/provider-traces/auth-foundation/source.mjs': '2cd4d25cb341f60f20acec19905559c1590c3225b483c1616d86c742a58b4ea5',
 'conformance/provider-traces/images-runtime/README.md': '41ce4cb8eb44824bbd6360c48042b423cb2b02d69f67325de756c5f04fe5b536',
 'conformance/provider-traces/images-runtime/causal-check.py': '61fd6d05414787c39c1174e94d76537562caf591d2bbc82d7af3178660e91659',
 'conformance/provider-traces/images-runtime/source.mjs': 'd516568277ff95f480600db10c086d8e7863f67578e493bc60b39d21837d9265',
 'conformance/provider-traces/images-runtime/trace.json': '51eaabc698e7b771a926551426e37f5b511a1828d46cb7c9037fe990b4524dc7',
 'conformance/provider-traces/oauth-cli/README.md': 'a78f7542c2b7b67549006b96baee373559c332adc039f7f822a1ed8d877803ad',
 'conformance/provider-traces/oauth-cli/causal-observations.json': 'ce25ceb99552e541c49b59f8c3ed067f27bef93de3de23626819bccfe5ea169e',
 'conformance/provider-traces/oauth-cli/observations.json': 'e2f13e103676219d1684747db8480a7c6324be96202bf47e69581c0a75aa9f6d',
 'conformance/provider-traces/oauth-cli/rust-source-hashes.json': 'f9959a1892edd3b0d1b7e03fe06640bad68919c545f58823c51af16491912724',
 'conformance/provider-traces/oauth-cli/source.mjs': '889a8d9bde3d4478de3d196a7794861653ac633deceb19ce2d2020e508d086e7',
 'conformance/provider-traces/oauth-selected/README.md': '3498a586e3072afe119966b05956a2cbca131e88f386c490a8bcd9ea2031a877',
 'conformance/provider-traces/oauth-selected/source-observations.json': '1b99806d8afada3bd7865cd01746223ada52fc49cb5629c612762f6059fcf194',
 'conformance/provider-traces/oauth-selected/source.mjs': 'ec6a679ee7627fb100e7a50350b2b4cece2c4108f85e2ab85339c05cd6be76c8',
 'conformance/provider-traces/openai-codex-responses/README.md': 'ebbec1881d6dc37c36f7c433eb598e539ff3e126fad1ed21a3265e77ab9b2e85',
 'conformance/provider-traces/openai-codex-responses/account-oracle.mjs': '9b3c8db92b5e3abcb5913732ef0b9f80bc2be078d6acd400ae357c3027d3a2da',
 'conformance/provider-traces/openai-codex-responses/account.json': 'a358af79002237993c59c8e9e6f220b15e732ad4f4baf11b8b7457f165dbacbf',
 'conformance/provider-traces/openai-codex-responses/cache-oracle.mjs': 'b78fb463a37e40c83db5e9c08c0211ce1ca0c2639a08a335d39746315b1a7ae4',
 'conformance/provider-traces/openai-codex-responses/cache.json': '93c4a17f47cf4cfc4b72c0d50ed84746c1c236ce1cb8bc4ecf9a399bc3385e00',
 'conformance/provider-traces/openai-codex-responses/harness.mjs': 'b53b2e660b5ce71307c80b4f06c52c9b6fb8b433daa5e283dbdf580c2e60fef7',
 'conformance/provider-traces/openai-codex-responses/metadata-oracle.mjs': '2667337ba30dc9c5cd781da31338ffdf94817bf2dd71026ac7b6aff99becf5a0',
 'conformance/provider-traces/openai-codex-responses/metadata.json': 'eb7c9216f8f0f5786d74ceee12d8a4f6028775ec99f2d5ad4e3ac2793a590d3f',
 'conformance/provider-traces/openai-codex-responses/native-frame-oracle.mjs': '92411a5cc36492f923d1ed6ef5810415af30012ab983e492e3b2dcc3983c2765',
 'conformance/provider-traces/openai-codex-responses/native-frames.json': '957253279a69d86f87e485966444980eb8f0b24832a1401106a9bd5eb857cb35',
 'conformance/provider-traces/openai-codex-responses/oracle.mjs': 'af636e21bf5e4f7a7b3855ea1dd567aa876fa62308b32e9aae14b0092c518140',
 'conformance/provider-traces/openai-codex-responses/payload-oracle.mjs': 'd188ed8a4bb6614c09bb87682b02b231aa19fc5764cecff0e59ddcce002c46ca',
 'conformance/provider-traces/openai-codex-responses/payload.json': '4278a16723cb5634252566e81eab8eb9e944e8e5552f63630839304be129793e',
 'conformance/provider-traces/openai-codex-responses/review-correction-oracle.mjs': '97760e5a824c3a5b545e3d1e04695d34d5237948899ebf9fef093c5993508d6a',
 'conformance/provider-traces/openai-codex-responses/review-correction.json': '2d4ff3e6284ff2f86017f969877b24dc4e2863fa5bb04aab7b83f1a82a62948b',
 'conformance/provider-traces/openai-codex-responses/source-comparison-check.mjs': '5bb735d2919b275e052e1b1e48c22f9554a91d1ea62e47cb9c968dadf126a699',
 'conformance/provider-traces/openai-codex-responses/source-comparison.mjs': 'e63fea958af4093f59332c6f2821a5a13a025265b44d62788c5d4674ab904f31',
 'conformance/provider-traces/openai-codex-responses/timeout-oracle.mjs': '65b0d0e2565ffde3cff88fa3757acf64c5f05481d1af91e6c2442d0e5d1229e1',
 'conformance/provider-traces/openai-codex-responses/timeout.json': '760f3f3999dec1adc0654957f77a1dfc92cb5638e1cbb1d2a434777196dbf6f5',
 'conformance/provider-traces/openai-codex-responses/trace.json': '3eb690370fb92dfb9de16a4e5ce89110983cbd9286550f72ede0118b13ca62f9',
 'conformance/provider-traces/openrouter-images/README.md': 'df806a631f6b343d1802baf5bc1953a0b8d6a620411d2d88c17bf86ffcb786f8',
 'conformance/provider-traces/openrouter-images/causal-check.py': 'f0962c31f6a001f62b284101ff76b16f9eeda745c2f3c42ee38a7da3d4d744cd',
 'conformance/provider-traces/openrouter-images/source.mjs': '12e85a06395cdfcf9e140a4ff827864a9aa987bd26b01a0c30f3eca4698c02a9',
 'conformance/provider-traces/openrouter-images/trace.json': '846ff8e5964d5efcd287a1a177fe07c68046522dd6a504df7dad3961e2370ca4',
 'conformance/providers/selected/linked.py': '05bcaf7c8421a2fdc912124b193362c184603d204b016490844b2ef5950c2db8',
 'conformance/providers/selected/source.mjs': 'd3f55dfa20f27c3c851fd4a29d47829b8f2c85df619a0c2402e3e7405a6396ac',
 'conformance/providers/selected/trace.json': '57858b2626b33180579494aca07bd87cf8565cefe01fe80697995686975398b3',
 'conformance/upstream.mjs': '3e15611a45f97a6bf3f99b017270ac03a48640da0ac000852647de6174a8cfd2',
 'crates/pi-ai-transport/examples/loopback_tls.rs': '5b57ca9bbb939807cefd02e95ffb5d2ba41d14aa42259f17c57b1f2a87c6389b',
 'scripts/generate_selected_catalogs.py': 'c7bd8d222095710a4cf9b7ac2fdd5a0f7cbe62016eb2bfaca36f4e99247883b2',
 'scripts/upstream.py': '338ee17d061330fa7649281af7f2f0d322bd77292b5dcd416b7e020a4da7e5f9'}

class ClosureError(ValueError):
    """A fixed S05 evidence relation or executed obligation changed."""


def policy_json(value):
    """Canonical JSON retains bool/integer/float distinctions unlike Python equality."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(policy_json(value).encode()).hexdigest()


def expanded(row):
    value = copy.deepcopy(row)
    value["tests"] = [copy.deepcopy(TEST_POLICY[key]) for key in value["tests"]]
    if "occurrences" in value:
        value["occurrences"] = [expanded(entry) for entry in value["occurrences"]]
    return value


def expected_catalog():
    return {
        "schemaVersion": 1, "sourceCommit": PIN, "scopeDigest": SCOPE_DIGEST,
        "symbols": [{"id": key, **expanded(value)} for key, value in SYMBOL_POLICY.items()],
        "sources": [{"id": key, **copy.deepcopy(value)} for key, value in SOURCE_POLICY.items()],
        "composition": [{"id": key, **expanded(value)} for key, value in COMPOSITION_POLICY.items()],
        "testFiles": {f"crates/{t['package']}/tests/{t['target']}.rs": t["sha256"] for t in TEST_POLICY.values()},
        "evidenceFiles": copy.deepcopy(EVIDENCE_POLICY),
    }


def require(condition, message):
    if not condition:
        raise ClosureError(message)


def catalog(root=ROOT, document=None, expected_digest=None, check_files=True):
    if document is None:
        document = json.loads((root / CATALOG).read_text())
    require(type(document) is dict and type(document.get("schemaVersion")) is int and document["schemaVersion"] == 1,
            "S05 schema identity requires exact integer version 1")
    require(digest(document) == (CATALOG_DIGEST if expected_digest is None else expected_digest), "S05 catalog digest changed")
    # Independent literal policy remains binding even after deliberate catalog rehash.
    require(policy_json(document) == policy_json(expected_catalog()), "S05 independent type-preserving route/probe/source/composition policy changed")
    if check_files:
        for field in ("testFiles", "evidenceFiles"):
            for path, expected in document[field].items():
                require((root / path).is_file() and hashlib.sha256((root / path).read_bytes()).hexdigest() == expected, "S05 evidence file changed: " + path)
    return document


def binding(rule, symbol, document):
    identity = symbol["origin"] + ":" + symbol["name"]
    entry = next((e for e in document["symbols"] if e["id"] == identity), None)
    require(entry is not None, "S05 symbol binding missing")
    key = symbol["subpath"] + ":" + symbol["name"]
    occurrence = next((e for e in entry["occurrences"] if e["key"] == key), None)
    require(occurrence is not None, "S05 occurrence binding missing: " + key)
    require(rule.get("evidence") == "s05-export:" + identity and rule.get("disposition") == "implemented", "S05 disposition/evidence mismatch")
    require(symbol["kind"] == entry["sourceKind"], "S05 source kind changed")
    require(rule.get("rust_target") == occurrence["routeTarget"] and rule.get("features") == occurrence["routeFeatures"] and rule.get("package") == occurrence["routePackage"], "S05 exact occurrence route/profile changed")
    return entry, occurrence


def probe_profiles(document):
    profiles = collections.defaultdict(dict)
    for symbol in document["symbols"]:
        entries = [(symbol["owningPackage"], symbol["owningFeatures"], symbol["owningTarget"], symbol["owningProbe"])]
        entries += [(r["routePackage"], r["routeFeatures"], r["routeTarget"], r["routeProbe"]) for r in symbol["occurrences"]]
        for package, features, target, body in entries:
            require(body.count("$target") == 1, "S05 target use count changed")
            key = (target, symbol["sourceKind"])
            profile = profiles[(package, tuple(features))]
            require(key not in profile or profile[key] == body, "S05 conflicting shared target probe")
            profile[key] = body
    return profiles


def probe_source(entries):
    approved = {}
    for policy in SYMBOL_POLICY.values():
        approved[(policy["owningTarget"], policy["sourceKind"])] = policy["owningProbe"]
        for route in policy["occurrences"]:
            approved[(route["routeTarget"], policy["sourceKind"])] = route["routeProbe"]
    require(entries and all(approved.get(key) == body for key, body in entries.items()), "S05 independent typed probe admission failed")
    blocks = ["#![allow(dead_code,unused_imports)]"]
    for index, ((target, _kind), body) in enumerate(sorted(entries.items())):
        require(body.count("$target") == 1, "S05 probe target missing or repeated")
        blocks.append("mod p" + str(index) + "{use std::{sync::Arc,future::Future};use pi_ai_core::*;use pi_ai_providers::{auth,images as img};" + body.replace("$target", target) + "}")
    return "\n".join(blocks) + "\nfn main(){}\n"


def local_feature_closure(root, package, features, manifests=None):
    manifests = {} if manifests is None else manifests
    enabled = collections.defaultdict(set)
    todo = [(package, f) for f in features]
    while todo:
        owner, feature = todo.pop()
        if feature in enabled[owner]:
            continue
        enabled[owner].add(feature)
        manifest = manifests.get(owner)
        if manifest is None:
            manifest = tomllib.loads((root / "crates" / owner / "Cargo.toml").read_text())
            manifests[owner] = manifest
        for activation in manifest.get("features", {}).get(feature, []):
            if activation.startswith("dep:"):
                continue
            if "/" in activation:
                dependency, value = activation.split("/", 1)
                if dependency.startswith("pi-"):
                    todo.append((dependency.rstrip("?"), value))
            else:
                todo.append((owner, activation))
    # Every probe directly names providers with defaults disabled. Its accepted
    # nonoptional transport edge still activates the empty transport default marker.
    enabled["pi-ai-transport"].add("default")
    if package == "pi-ai" and features:
        enabled["pi-ai-providers"].add("default")
    return {k: sorted(v) for k, v in enabled.items()}


def validate_feature_closure(metadata, expected):
    packages = {p["id"]: p["name"] for p in metadata["packages"]}
    actual = {packages[node["id"]]: sorted(node["features"]) for node in metadata["resolve"]["nodes"] if packages[node["id"]] in expected}
    require(actual == expected, "S05 measured feature forwarding changed: " + repr({"actual":actual,"expected":expected}))


def compile_profile(package, features, entries, root=ROOT):
    with tempfile.TemporaryDirectory(prefix="pi-s05-export-") as directory:
        project = Path(directory)
        (project / "src").mkdir()
        manifest = '[package]\nname="s05-export-probe"\nversion="0.0.0"\nedition="2024"\n[workspace]\n[dependencies]\n'
        for name in ("pi-ai", "pi-ai-providers", "pi-ai-core"):
            chosen = features if name == package else []
            manifest += name + "={path=" + json.dumps(str(root / "crates" / name)) + ",default-features=false,features=" + json.dumps(list(chosen)) + "}\n"
        (project / "Cargo.toml").write_text(manifest)
        (project / "src/main.rs").write_text(probe_source(entries))
        env = {**os.environ, "CARGO_NET_OFFLINE": "true", "CARGO_TARGET_DIR": str(root / "target/ai-conformance-probe"), "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0", "CARGO_BUILD_JOBS": "2"}
        command = ["cargo", "metadata", "--format-version=1", "--offline", "--manifest-path", str(project / "Cargo.toml")]
        result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True)
        require(result.returncode == 0, "S05 profile metadata failed:\n" + result.stderr)
        expected = FEATURE_POLICY[(package, tuple(features))]
        validate_feature_closure(json.loads(result.stdout), expected)
        result = subprocess.run(["cargo", "check", "--offline", "--manifest-path", str(project / "Cargo.toml")], cwd=root, env=env, capture_output=True, text=True)
        require(result.returncode == 0, "S05 exact typed probe failed (" + package + "/" + ",".join(features) + "):\n" + result.stderr)


def test_key(test):
    return (test["package"], test["target"], test["kind"], test["name"], tuple(test["features"]))


def test_profiles(document):
    profiles = collections.defaultdict(dict)
    references = []
    for symbol in document["symbols"]:
        references += symbol["tests"]
        for route in symbol["occurrences"]:
            references += route["tests"]
    for entry in document["composition"]:
        references += entry["tests"]
    for t in references:
        profiles[(t["package"], tuple(t["features"]))][test_key(t)] = t
    return profiles


def select_artifacts(messages, package_id, targets):
    artifacts = collections.defaultdict(list)
    for message in messages:
        target = message.get("target", {})
        if message.get("reason") != "compiler-artifact" or target.get("name") not in targets:
            continue
        require(message.get("package_id") == package_id, "S05 wrong-package test artifact")
        require(target.get("kind") == [targets[target["name"]]] and message.get("profile", {}).get("test") is True, "S05 wrong-kind/profile test artifact")
        if message.get("executable"):
            artifacts[target["name"]].append(message["executable"])
    require(set(artifacts) == set(targets) and all(len(values) == 1 for values in artifacts.values()), "S05 missing or ambiguous test executable")
    return {name: values[0] for name, values in artifacts.items()}


def executed_test(output, name):
    require(len(re.findall(r"^test " + re.escape(name) + r" \.\.\. ok$", output, re.M)) == 1, "S05 named test did not execute once: " + name)
    require(len(re.findall(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", output)) == 1, "S05 test ignored/filtered/empty result")


def execute_tests(package, features, tests, root=ROOT):
    approved = {test_key(t): t for t in TEST_POLICY.values()}
    require(tests and len({test_key(t) for t in tests}) == len(tests), "S05 duplicate or empty requested tests")
    require(all(approved.get(test_key(t)) == t and t["package"] == package and t["features"] == list(features) for t in tests), "S05 independent test/package/profile admission failed")
    metadata = subprocess.run(["cargo", "metadata", "--format-version=1", "--no-deps", "--offline"], cwd=root, capture_output=True, text=True)
    require(metadata.returncode == 0, "S05 local package metadata failed")
    packages = [p for p in json.loads(metadata.stdout)["packages"] if p["name"] == package and Path(p["manifest_path"]).resolve() == (root / "crates" / package / "Cargo.toml").resolve()]
    require(len(packages) == 1, "S05 requested local package identity missing")
    targets = {t["target"]: t["kind"] for t in tests}
    command = ["cargo", "test", "-p", package, "--no-default-features", "--locked", "--offline", "--no-run", "--message-format=json"]
    if features:
        command += ["--features", ",".join(features)]
    for target, kind in sorted(targets.items()):
        require(kind == "test", "S05 unapproved test target kind")
        command += ["--test", target]
    result = subprocess.run(command, cwd=root, capture_output=True, text=True)
    require(result.returncode == 0, "S05 test profile build failed:\n" + result.stderr)
    messages = []
    for line in result.stdout.splitlines():
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    artifacts = select_artifacts(messages, packages[0]["id"], targets)
    for test in sorted(tests, key=test_key):
        result = subprocess.run([artifacts[test["target"]], "--exact", test["name"], "--nocapture"], cwd=root, capture_output=True, text=True)
        require(result.returncode == 0, "S05 required test failed: " + str(test_key(test)) + "\n" + result.stdout + result.stderr)
        executed_test(result.stdout, test["name"])


def execute_sources(sources, root=ROOT):
    for source in sources:
        identity = source.get("id")
        require(identity in SOURCE_POLICY and source == {"id": identity, **SOURCE_POLICY[identity]}, "S05 independent source command admission failed")
        require(hashlib.sha256((root / source["script"]).read_bytes()).hexdigest() == source["scriptSha256"], "S05 source program fingerprint changed")
        require(hashlib.sha256((root / source["expected"]).read_bytes()).hexdigest() == EVIDENCE_POLICY[source["expected"]], "S05 checked source artifact fingerprint changed")
        command = [source["executable"], *source["beforeScript"], str(root / source["script"]), *source["args"]]
        result = subprocess.run(command, cwd=root, env=upstream_environment({"PATH": "/usr/bin:/bin", "NODE_NO_WARNINGS": "1"}, root=root), capture_output=True, text=True)
        require(result.returncode == 0, "S05 complete source command failed: " + source["id"] + "\n" + result.stderr)
        if source["mode"] == "json":
            try:
                observed = json.loads(result.stdout)
                expected = json.loads((root / source["expected"]).read_text())
            except json.JSONDecodeError as error:
                raise ClosureError("S05 source stdout is not JSON") from error
            require(json.dumps(observed, ensure_ascii=False, separators=(",", ":")) == json.dumps(expected, ensure_ascii=False, separators=(",", ":")), "S05 exact source observation changed")
        else:
            require(source["mode"] == "check" and source["args"] == ["--check"] and result.stdout == source["stdout"], "S05 approved source check output changed")


def verify_binaries(document, root=ROOT):
    manifest = tomllib.loads((root / "crates/pi-ai/Cargo.toml").read_text())
    targets = {t["name"]: t for t in manifest.get("bin", [])}
    for entry in document["composition"]:
        for target in entry.get("binaries", []):
            actual = targets.get(target["name"])
            require(actual is not None and actual["path"] == target["path"] and actual.get("required-features") == target["requiredFeatures"], "S05 developer binary admission changed")


def counts(document):
    owning = {(s["owningTarget"], s["sourceKind"]) for s in document["symbols"]}
    routes = {(r["routeTarget"], s["sourceKind"]) for s in document["symbols"] for r in s["occurrences"]}
    return {"symbols": len(document["symbols"]), "occurrences": sum(len(s["occurrences"]) for s in document["symbols"]), "owning": len(owning), "routes": len(routes), "union": len(owning | routes)}


def verify(root=ROOT):
    document = catalog(root)
    verify_binaries(document, root)
    for (package, features), entries in sorted(probe_profiles(document).items()):
        compile_profile(package, features, entries, root)
    for (package, features), tests in sorted(test_profiles(document).items()):
        execute_tests(package, features, list(tests.values()), root)
    execute_sources(document["sources"], root)
    print("verified S05 exports/composition: " + json.dumps(counts(document), sort_keys=True) + "; compositions=4; sources=12")

# Independently frozen local feature closure; never inferred from candidate metadata.
FEATURE_POLICY = {('pi-ai', ()): {'pi-ai': [], 'pi-ai-core': [], 'pi-ai-providers': [], 'pi-ai-transport': ['default']},
 ('pi-ai', ('all-providers',)): {'pi-ai': ['all-providers',
                                           'catalog-anthropic',
                                           'catalog-deepseek',
                                           'catalog-google',
                                           'catalog-openai',
                                           'catalog-openai-codex',
                                           'catalog-openrouter',
                                           'provider-anthropic',
                                           'provider-deepseek',
                                           'provider-google',
                                           'provider-openai',
                                           'provider-openai-codex',
                                           'provider-openrouter'],
                                 'pi-ai-core': [],
                                 'pi-ai-providers': ['catalog-anthropic',
                                                     'catalog-deepseek',
                                                     'catalog-google',
                                                     'catalog-openai',
                                                     'catalog-openai-codex',
                                                     'catalog-openrouter',
                                                     'default',
                                                     'provider-anthropic',
                                                     'provider-deepseek',
                                                     'provider-google',
                                                     'provider-openai',
                                                     'provider-openai-codex',
                                                     'provider-openrouter',
                                                     'wire-anthropic-messages',
                                                     'wire-google-genai',
                                                     'wire-openai-codex-responses',
                                                     'wire-openai-completions',
                                                     'wire-openai-responses'],
                                 'pi-ai-transport': ['default']},
 ('pi-ai', ('catalog-openai',)): {'pi-ai': ['catalog-openai'],
                                  'pi-ai-core': [],
                                  'pi-ai-providers': ['catalog-openai', 'default'],
                                  'pi-ai-transport': ['default']},
 ('pi-ai', ('catalog-openrouter-images',)): {'pi-ai': ['catalog-openrouter-images'],
                                             'pi-ai-core': [],
                                             'pi-ai-providers': ['catalog-openrouter-images', 'default'],
                                             'pi-ai-transport': ['default']},
 ('pi-ai', ('compat',)): {'pi-ai': ['catalog-anthropic',
                                    'catalog-deepseek',
                                    'catalog-google',
                                    'catalog-openai',
                                    'catalog-openai-codex',
                                    'catalog-openrouter',
                                    'catalog-openrouter-images',
                                    'compat',
                                    'images-openrouter',
                                    'images-runtime',
                                    'provider-anthropic',
                                    'provider-deepseek',
                                    'provider-google',
                                    'provider-openai',
                                    'provider-openai-codex',
                                    'provider-openrouter'],
                          'pi-ai-core': [],
                          'pi-ai-providers': ['catalog-anthropic',
                                              'catalog-deepseek',
                                              'catalog-google',
                                              'catalog-openai',
                                              'catalog-openai-codex',
                                              'catalog-openrouter',
                                              'catalog-openrouter-images',
                                              'default',
                                              'images-openrouter',
                                              'provider-anthropic',
                                              'provider-deepseek',
                                              'provider-google',
                                              'provider-openai',
                                              'provider-openai-codex',
                                              'provider-openrouter',
                                              'wire-anthropic-messages',
                                              'wire-azure-openai-responses',
                                              'wire-bedrock-converse-stream',
                                              'wire-google-genai',
                                              'wire-google-vertex',
                                              'wire-mistral-conversations',
                                              'wire-openai-codex-responses',
                                              'wire-openai-completions',
                                              'wire-openai-responses',
                                              'wire-openrouter-images',
                                              'wire-pi-messages'],
                          'pi-ai-transport': ['default']},
 ('pi-ai', ('images-openrouter',)): {'pi-ai': ['catalog-openrouter-images',
                                               'images-openrouter',
                                               'images-runtime'],
                                     'pi-ai-core': [],
                                     'pi-ai-providers': ['catalog-openrouter-images',
                                                         'default',
                                                         'images-openrouter',
                                                         'wire-openrouter-images'],
                                     'pi-ai-transport': ['default']},
 ('pi-ai', ('images-runtime',)): {'pi-ai': ['images-runtime'],
                                  'pi-ai-core': [],
                                  'pi-ai-providers': ['default'],
                                  'pi-ai-transport': ['default']},
 ('pi-ai-providers', ()): {'pi-ai': [],
                           'pi-ai-core': [],
                           'pi-ai-providers': [],
                           'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('catalog-anthropic',)): {'pi-ai': [],
                                               'pi-ai-core': [],
                                               'pi-ai-providers': ['catalog-anthropic'],
                                               'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('catalog-deepseek',)): {'pi-ai': [],
                                              'pi-ai-core': [],
                                              'pi-ai-providers': ['catalog-deepseek'],
                                              'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('catalog-google',)): {'pi-ai': [],
                                            'pi-ai-core': [],
                                            'pi-ai-providers': ['catalog-google'],
                                            'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('catalog-openai',)): {'pi-ai': [],
                                            'pi-ai-core': [],
                                            'pi-ai-providers': ['catalog-openai'],
                                            'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('catalog-openai-codex',)): {'pi-ai': [],
                                                  'pi-ai-core': [],
                                                  'pi-ai-providers': ['catalog-openai-codex'],
                                                  'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('catalog-openrouter',)): {'pi-ai': [],
                                                'pi-ai-core': [],
                                                'pi-ai-providers': ['catalog-openrouter'],
                                                'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('images-openrouter',)): {'pi-ai': [],
                                               'pi-ai-core': [],
                                               'pi-ai-providers': ['catalog-openrouter-images',
                                                                   'images-openrouter',
                                                                   'wire-openrouter-images'],
                                               'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('provider-anthropic',)): {'pi-ai': [],
                                                'pi-ai-core': [],
                                                'pi-ai-providers': ['catalog-anthropic',
                                                                    'provider-anthropic',
                                                                    'wire-anthropic-messages'],
                                                'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('provider-deepseek',)): {'pi-ai': [],
                                               'pi-ai-core': [],
                                               'pi-ai-providers': ['catalog-deepseek',
                                                                   'provider-deepseek',
                                                                   'wire-openai-completions'],
                                               'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('provider-google',)): {'pi-ai': [],
                                             'pi-ai-core': [],
                                             'pi-ai-providers': ['catalog-google',
                                                                 'provider-google',
                                                                 'wire-google-genai'],
                                             'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('provider-openai',)): {'pi-ai': [],
                                             'pi-ai-core': [],
                                             'pi-ai-providers': ['catalog-openai',
                                                                 'provider-openai',
                                                                 'wire-openai-responses'],
                                             'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('provider-openai-codex',)): {'pi-ai': [],
                                                   'pi-ai-core': [],
                                                   'pi-ai-providers': ['catalog-openai-codex',
                                                                       'provider-openai-codex',
                                                                       'wire-openai-codex-responses'],
                                                   'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('provider-openrouter',)): {'pi-ai': [],
                                                 'pi-ai-core': [],
                                                 'pi-ai-providers': ['catalog-openrouter',
                                                                     'provider-openrouter',
                                                                     'wire-openai-completions'],
                                                 'pi-ai-transport': ['default']},
 ('pi-ai-providers', ('wire-openrouter-images',)): {'pi-ai': [],
                                                    'pi-ai-core': [],
                                                    'pi-ai-providers': ['wire-openrouter-images'],
                                                    'pi-ai-transport': ['default']}}
