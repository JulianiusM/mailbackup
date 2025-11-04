# Coverage Improvement Plan

## Current Status

**Current Coverage**: 87% (1411/1622 lines covered)
**Target Coverage**: 95% (1541/1622 lines covered)
**Gap**: 130 lines need to be covered

## Modules Requiring Coverage Improvements

### Priority 1: High-Impact Modules

1. **uploader.py** - 63% (41 uncovered lines)
   - Lines 63-64: Email file existence check
   - Lines 71-74: Attachment copying logic
   - Lines 97-122: Upload_email function main logic
   - Lines 129-131, 138, 141-142, 148-159: Error handling and remote operations
   
   **Recommended tests**:
   - Test upload_email with missing email file
   - Test upload_email with missing attachments
   - Test upload_email with rclone failures
   - Test incremental_upload with various scenarios

2. **manifest.py** - 75% (40 uncovered lines)
   - Lines 83-84, 130-135: Queue restoration
   - Lines 146-150, 163-168: Synchronization logic
   - Lines 196, 226-244: Remote manifest handling
   - Lines 259-266: CSV parsing edge cases
   
   **Recommended tests**:
   - Test queue restoration from corrupted file
   - Test synchronization with remote manifest
   - Test merge_manifest with conflicts
   - Test CSV parsing with malformed data

3. **rotation.py** - 77% (26 uncovered lines)
   - Lines 93-95, 100-104: Archive extraction
   - Lines 106-110, 116-125: Merging logic
   - Lines 138-140: Error handling
   
   **Recommended tests**:
   - Test archive_year with corrupted archives
   - Test archive_year with merge conflicts
   - Test error handling during compression

### Priority 2: Medium-Impact Modules

4. **utils.py** - 88% (32 uncovered lines)
   - Lines 72-74, 98: File operations edge cases
   - Lines 135-142: Date parsing edge cases
   - Lines 164-171: Remote hash calculation
   - Lines 225-228, 297-298: Error handling
   
   **Recommended tests**:
   - Test atomic_upload_file failure modes
   - Test parse_mail_date with various invalid formats
   - Test compute_remote_sha256 with network errors

5. **db.py** - 89% (14 uncovered lines)
   - Lines 48-53, 58-59, 70-71: Error handling
   - Lines 142-143, 266-267: Edge cases
   
   **Recommended tests**:
   - Test database connection failures
   - Test query edge cases

6. **extractor.py** - 85% (22 uncovered lines)
   - Lines 46-48, 57, 61-65: Email parsing edge cases
   - Lines 90-92, 125-127: Attachment handling
   - Lines 133, 137-139: Error handling
   
   **Recommended tests**:
   - Test with malformed emails
   - Test with corrupted attachments

### Priority 3: Low-Impact Modules (Already High Coverage)

7. **executor.py** - 92% (14 uncovered lines) - Already good
8. **config.py** - 92% (10 uncovered lines) - Already good  
9. **__main__.py** - 86% (8 uncovered lines) - Already good
10. **integrity.py** - 97% (4 uncovered lines) - Excellent

## Implementation Strategy

### Phase 1: Quick Wins (Get to 90%)
Add tests for the most straightforward uncovered lines:
- utils.py error handling
- db.py connection edge cases
- extractor.py basic error cases

**Estimated impact**: +3-4% coverage

### Phase 2: Core Module Coverage (Get to 95%)
Focus on the three main modules:
- uploader.py: Add comprehensive upload failure tests
- manifest.py: Add queue and sync tests
- rotation.py: Add archive handling tests

**Estimated impact**: +5-6% coverage

### Phase 3: Edge Cases (Reach 95%+)
- Test malformed data handling
- Test network failure scenarios
- Test filesystem errors

**Estimated impact**: +1-2% coverage

## Test File Locations

Following the domain-based structure:
- uploader.py tests → `tests/unit/test_uploader.py` and `tests/integration/test_uploader.py`
- manifest.py tests → `tests/unit/test_manifest.py` and `tests/integration/test_manifest.py`
- rotation.py tests → `tests/unit/test_rotation.py` and `tests/integration/test_rotation.py`

## Example Test Patterns

### Testing Error Handling
```python
def test_function_with_missing_file(self, mocker):
    """Test function handles missing file gracefully."""
    mocker.patch("pathlib.Path.exists", return_value=False)
    result = function_under_test()
    assert result == expected_fallback_value
```

### Testing Network Failures
```python
def test_function_with_network_error(self, mocker):
    """Test function handles network errors."""
    mocker.patch("subprocess.run", side_effect=subprocess.CalledProcessError(1, "cmd"))
    with pytest.raises(Exception):
        function_under_test()
```

### Testing Malformed Data
```python
def test_function_with_invalid_input(self):
    """Test function with malformed input."""
    result = parse_function("invalid<>data")
    assert result == sanitized_or_default_value
```

## Notes

- Focus on **unit tests** first (easier to write, faster to run)
- Add **integration tests** for complex workflows
- Use **mocking** extensively to isolate code under test
- Follow the **domain-based structure** (one file per module)
- Each test should cover **2-5 lines** on average

## Estimated Effort

- **Phase 1**: 2-3 hours (add ~50 lines of test code)
- **Phase 2**: 4-6 hours (add ~100 lines of test code)
- **Phase 3**: 1-2 hours (add ~30 lines of test code)

**Total**: 7-11 hours to reach 95% coverage
