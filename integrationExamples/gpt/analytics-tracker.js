// External Analytics Tracker Script
// Provides filterData function and event listeners for Prebid.js

(function() {
    'use strict';
    
    // Common function to filter out sensitive data from event payloads
    function filterData(args) {
        function deepFilter(obj) {
            if (obj === null || typeof obj !== 'object') {
                return obj;
            }
            
            if (Array.isArray(obj)) {
                return obj.map(deepFilter);
            }
            
            const filtered = {};
            Object.keys(obj).forEach(key => {
                if (key === 'ad') {
                    // Remove ad field entirely
                    return;
                } else if (key === 'segment' && obj.data && Array.isArray(obj[key])) {
                    // Replace user.data.segment with empty array
                    filtered[key] = [];
                } else if (key === 'uids' && Array.isArray(obj[key])) {
                    // Replace uids with empty array
                    filtered[key] = [];
                } else if (key === 'metrics' && typeof obj[key] === 'object') {
                    // Replace metrics with empty object
                    filtered[key] = {};
                } else if (typeof obj[key] === 'object') {
                    filtered[key] = deepFilter(obj[key]);
                } else {
                    filtered[key] = obj[key];
                }
            });
            
            return filtered;
        }
        
        return deepFilter(args);
    }
    
    // Expose filterData globally for use by analytics adapter
    window.filterData = filterData;
    
    // Send event to analytics endpoint
    function sendAnalyticsEvent(eventData) {
        if (!window.ANALYTICS_CONFIG || window.ANALYTICS_CONFIG.useAdapter !== false) {
            return; // Only send if not using adapter
        }
        
        try {
            fetch(window.ANALYTICS_CONFIG.endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(eventData)
            }).catch(error => {
                console.warn('Analytics tracking failed:', error);
            });
        } catch (error) {
            console.warn('Analytics tracking error:', error);
        }
    }
    
    // Event handlers
    function handleAuctionInit(args) {
        const eventData = {
            eventType: 'auctionInit',
            timestamp: new Date().toISOString(),
            data: filterData(args)
        };
        sendAnalyticsEvent(eventData);
    }
    
    function handleBidRequested(args) {
        const eventData = {
            eventType: 'bidRequested',
            timestamp: new Date().toISOString(),
            data: filterData(args)
        };
        sendAnalyticsEvent(eventData);
    }
    
    function handleBidResponse(args) {
        const eventData = {
            eventType: 'bidResponse',
            timestamp: new Date().toISOString(),
            data: filterData(args)
        };
        sendAnalyticsEvent(eventData);
    }
    
    function handleBidWon(args) {
        const eventData = {
            eventType: 'bidWon',
            timestamp: new Date().toISOString(),
            data: filterData(args)
        };
        sendAnalyticsEvent(eventData);
    }
    
    // Initialize analytics tracking
    function initAnalyticsTracking() {
        if (typeof pbjs === 'undefined') {
            console.warn('Prebid.js not found, retrying analytics initialization...');
            setTimeout(initAnalyticsTracking, 100);
            return;
        }
        
        // Wait for Prebid.js to be ready
        pbjs.que.push(function() {
            // Only register event listeners if not using adapter
            if (window.ANALYTICS_CONFIG && window.ANALYTICS_CONFIG.useAdapter === false) {
                console.log('External analytics tracker initialized');
                
                // Register event listeners
                pbjs.onEvent('auctionInit', handleAuctionInit);
                pbjs.onEvent('bidRequested', handleBidRequested);
                pbjs.onEvent('bidResponse', handleBidResponse);
                pbjs.onEvent('bidWon', handleBidWon);
            }
        });
    }
    
    // Auto-initialize when script loads
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initAnalyticsTracking);
    } else {
        initAnalyticsTracking();
    }
    
})();