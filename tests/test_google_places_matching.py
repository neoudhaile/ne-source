"""Google Places matching should be conservative for CSV imports."""
from unittest.mock import patch


def test_find_place_rejects_candidate_with_wrong_name_and_address():
    import pipeline.google_places as mod

    places = [{
        'id': 'bad-place',
        'displayName': {'text': 'West Basin Municipal Water District'},
        'formattedAddress': '17140 S Avalon Blvd, Carson, CA 90746',
    }]

    with patch.object(mod, 'search_text', return_value=(places, None)), \
         patch.object(mod, 'get_place_details') as mock_details:
        result = mod.find_place(
            'Western Municipal Water District',
            address='14205 Meridian Parkway',
            city='March Air Reserve Base',
            state='CA',
        )

    assert result is None
    mock_details.assert_not_called()


def test_find_place_accepts_close_name_and_address_match():
    import pipeline.google_places as mod

    places = [{
        'id': 'good-place',
        'displayName': {'text': 'Western Municipal Water District'},
        'formattedAddress': '14205 Meridian Parkway, March Air Reserve Base, CA 92518',
    }]
    details = {
        'id': 'good-place',
        'displayName': {'text': 'Western Municipal Water District'},
        'formattedAddress': '14205 Meridian Parkway, March Air Reserve Base, CA 92518',
        'websiteUri': 'https://example.com',
    }

    with patch.object(mod, 'search_text', return_value=(places, None)), \
         patch.object(mod, 'get_place_details', return_value=details) as mock_details:
        result = mod.find_place(
            'Western Municipal Water District',
            address='14205 Meridian Parkway',
            city='March Air Reserve Base',
            state='CA',
        )

    assert result == details
    mock_details.assert_called_once_with('good-place')
