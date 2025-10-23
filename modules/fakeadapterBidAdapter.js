/**
 * Fake bid adapter for testing - always returns bids
 * Uses Prebid's buildRequests return format to provide bids without server calls
 */
import { registerBidder } from '../src/adapters/bidderFactory.js';
import { BANNER } from '../src/mediaTypes.js';

const BIDDER_CODE = 'fakeadapter';

export const spec = {
  code: BIDDER_CODE,
  supportedMediaTypes: [BANNER],

  isBidRequestValid: function(bid) {
    return true; // Always valid
  },

  buildRequests: function(validBidRequests, bidderRequest) {
    console.log('FakeAdapter buildRequests:', validBidRequests);

    // Create separate requests for each bid to handle them individually
    return validBidRequests.map(bidRequest => {
      // Generate random integer CPM between 1 and 5, formatted as float
      const cpm = parseFloat((Math.floor(Math.random() * 5) + 1).toFixed(2));

      // Get size from bid request
      let width = 300;
      let height = 250;

      if (bidRequest.sizes && bidRequest.sizes.length > 0) {
        [width, height] = bidRequest.sizes[0];
      } else if (bidRequest.mediaTypes && bidRequest.mediaTypes.banner && bidRequest.mediaTypes.banner.sizes) {
        [width, height] = bidRequest.mediaTypes.banner.sizes[0];
      }

      const mockBid = {
        requestId: bidRequest.bidId,
        cpm: cpm,
        width: width,
        height: height,
        ad: `<div style="width:${width}px;height:${height}px;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);display:flex;align-items:center;justify-content:center;color:white;font-family:Arial,sans-serif;font-size:20px;font-weight:bold;text-align:center;box-shadow:0 4px 6px rgba(0,0,0,0.1);border-radius:4px;"><div><div style="font-size:24px;">🎯</div><div style="margin-top:8px;">FAKE ADAPTER</div><div style="font-size:16px;margin-top:12px;">CPM: $${cpm}</div><div style="font-size:14px;margin-top:8px;">${width}x${height}</div></div></div>`,
        ttl: 3600,
        creativeId: 'fake-creative-' + Date.now(),
        netRevenue: true,
        currency: 'USD',
        mediaType: 'banner',
        meta: {
          advertiserDomains: ['fakeadvertiser.example.com']
        },
        // Optional fields that might help
        adUnitCode: bidRequest.adUnitCode,
        bidderCode: BIDDER_CODE,
        adId: bidRequest.bidId + '_' + Date.now()
      };

      console.log('FakeAdapter created bid:', mockBid);

      // Return a request object with the bid data embedded
      // Use a reliable fast endpoint
      return {
        method: 'GET',
        url: 'https://3pctest.pages.dev/',
        data: {
          bidRequest: bidRequest,
          mockBid: mockBid
        },
        options: {
          withCredentials: false
        }
      };
    });
  },

  interpretResponse: function(serverResponse, request) {
    console.log('FakeAdapter interpretResponse:', serverResponse, request);

    // Return the pre-generated bid from the request data
    const bid = request.data?.mockBid;
    if (!bid) {
      console.warn('FakeAdapter: No mock bid found in request');
      return [];
    }

    console.log('FakeAdapter returning bid:', bid);
    return [bid];
  },

  getUserSyncs: function(syncOptions, serverResponses, gdprConsent, uspConsent) {
    return [];
  }
};

registerBidder(spec);
