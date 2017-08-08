/**
 * Example Javascript visualization.
 *
 * The name of this file must be as specified by the opName parameter when you create the JavascriptVisualModel
 * with a ".js" extension.
 *
 * Uses Dojo's AMD (Asynchronous Module Definition) API define this code as a module and to load two modules to simplify
 * code: dom-construct and dom-geometry.
 *
 * A minimal Javascript file would be
 *
 * <code>
 *     define(function() {
 *         return {
 *             execute: function(outpane, visualData) {
 *                 // your javascript visualization code here
 *             }
 *         };
 *     });
 * </code>
 */
define([
    "dojo/dom-construct",
    "dojo/dom-geometry"
], function(domConstruct, domGeo) {
    return {
        /**
         * execute is the function that will be called by the custom operator Javascript framework.
         * @param outpane The enclosing html <div> in which your Javascript code should place elements into
         * @param visualData Data specified by your custom operator in createVisualResults() when creating the JavascriptVisualModel
         */
        execute: function(outpane, visualData) {
            var geo = domGeo.position(outpane);
            var canvas = domConstruct.create('canvas', {width: geo.w, height: geo.h}, outpane);
            var context = canvas.getContext('2d');
            // Trivial visualization example - all elements in visualData (an array) are randomly positioned in the canvas with random colors
            for (var i = 0; i < visualData.length; i++) {
                context.fillStyle = 'rgb('+Math.floor(Math.random() * 255)+','+Math.floor(Math.random() * 255)+','+Math.floor(Math.random() * 255)+')';
                context.fillText(visualData[i], 100 + (Math.random() * (geo.w - 200)), 100 + (Math.random() * (geo.h - 200)));
            }
        },
        simpleFunction: function(outpane, visualData) {
            var span = domConstruct.create('span', {}, outpane);
            span.innerHTML = "Hello World"
        }
    };
});