# Fake Adapter

## Overview

Module Name: Fake Adapter
Module Type: Bidder Adapter
Maintainer: testing@example.com

## Description

A test bid adapter that always returns bids for testing purposes. This adapter generates mock bids with random CPM values and colorful creative markup.

**Note:** This adapter is for testing only and should not be used in production.

## Configuration

The fake adapter accepts any parameters in the params object. All bid requests are considered valid.

```javascript
var adUnits = [{
    code: 'test-div',
    mediaTypes: {
        banner: {
            sizes: [[300, 250], [728, 90]]
        }
    },
    bids: [{
        bidder: 'fakeadapter',
        params: {
            placementId: 'test-placement' // Any params work
        }
    }]
}];
```

## Features

- Always returns valid bids
- Generates random CPM values between $2.00 and $10.00
- Creates visual creative markup with gradient backgrounds
- Supports all banner sizes
- No actual network requests made (uses mock endpoint)
