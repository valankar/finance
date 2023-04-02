#!/usr/bin/env python3
"""Get estimated home values."""

import common
import merge_redfin

# If there is a new value difference of this much, repopulate all old data.
# See https://twitter.com/valankar/status/1586667545486057472.
HISTORICAL_MERGE_THRESHOLD = 1000


def main():
    """Main."""
    historical_merge_required = False
    for filename, url in merge_redfin.URLS.items():
        value = merge_redfin.get_redfin_estimate(url)
        output = common.PREFIX + filename
        if not value:
            continue
        with open(output, encoding='utf-8') as input_file:
            old_value = float(input_file.read())
            if abs(value - old_value) > HISTORICAL_MERGE_THRESHOLD:
                print(f'{filename} old: {old_value} new: {value}')
                historical_merge_required = True
        with common.temporary_file_move(output) as output_file:
            output_file.write(str(value))

    if historical_merge_required:
        print('Redfin data differs by threshold, doing historical merge.')
        merge_redfin.main()


if __name__ == '__main__':
    main()
