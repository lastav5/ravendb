import d3 = require("d3");
import rbush = require("rbush");
import app = require("durandal/app");
import generalUtils = require("common/generalUtils");
import fileDownloader = require("common/fileDownloader");
import viewModelBase = require("viewmodels/viewModelBase");
import gapFinder = require("common/helpers/graph/gapFinder");
import graphHelper = require("common/helpers/graph/graphHelper");
import getIOMetricsCommand = require("commands/database/debug/getIOMetricsCommand");
import messagePublisher = require("common/messagePublisher");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleIndexes" | "trackItem" | "gapItem";
    arg: any;
}

// TODO: change to a general 'TimeGap' interface, for both IOStats & IndexingPerformance viewmodels
interface IndexingPerformanceGap {
    DurationInMilliseconds: number;
    StartTime: string;
}

class hitTest {
    cursor = ko.observable<string>("auto");   
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;    
    private onToggleIndexes: () => void;
    private handleTrackTooltip: (item: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) => void;
    private handleGapTooltip: (item: IndexingPerformanceGap, x: number, y: number) => void;
    private removeTooltip: () => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleIndexes: () => void,
        handleTrackTooltip: (item: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) => void,
        handleGapTooltip: (item: IndexingPerformanceGap, x: number, y: number) => void,
        removeTooltip: () => void) {
        this.container = container;
        this.onToggleIndexes = onToggleIndexes;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Server.Documents.Handlers.IOMetricsRecentStats) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "trackItem",
            arg: element
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerIndexToggle(x: number, y: number, width: number, height: number) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "toggleIndexes"
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerGapItem(x: number, y: number, width: number, height: number, element: IndexingPerformanceGap) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "gapItem",
            arg: element
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    onClick() {
        const clickLocation = d3.mouse(this.container.node());

        if ((d3.event as any).defaultPrevented) {
            return;
        }

        const items = this.findItems(clickLocation[0], clickLocation[1]);

        for (let i = 0; i < items.length; i++) {
            const item = items[i];

            if (item.actionType === "toggleIndexes") {
                this.onToggleIndexes();
            }
        }
    }

    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Server.Documents.Handlers.IOMetricsRecentStats)[0];
        if (currentItem) {
            this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);          
        }
        else {
            const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as IndexingPerformanceGap)[0];
            if (currentGapItem) {
                this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
            }
            else {
                this.removeTooltip();
            }
        }
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - metrics.brushSectionHeight,
            maxY: y - metrics.brushSectionHeight
        });
    }
}

class metrics extends viewModelBase {

    /* static */

    static readonly colors = {
        axis: "#546175",
        gaps: "#ca1c59",              
        trackBackground: "#2c343a",       
        trackNameBg: "rgba(57, 67, 79, 0.8)",
        trackNameFg: "#98a7b7",
        openedTrackArrow: "#ca1c59",
        closedTrackArrow: "#98a7b7"        
    }

    static readonly eventsColors = {       
        "JournalWriteLowSizeColor": "#38761d",
        "JournalWriteMedSizeColor": "#6aa84f",
        "JournalWriteHighSizeColor": "#93c47d",
        "DataFlushLowSizeColor": "#085394",
        "DataFlushMedSizeColor": "#597eaa",
        "DataFlushHighSizeColor": "#6fa8dc",
        "DataSyncLowSizeColor": "#b45f06",
        "DataSyncMedSizeColor": "#e69138",
        "DataSyncHighSizeColor": "#f6b26b"
    }
   
    private static readonly trackHeight = 18; 
    private static readonly trackMargin = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly openedTrackPadding = 4;
    private static readonly closedTrackHeight = metrics.closedTrackPadding + metrics.trackHeight + metrics.closedTrackPadding;
    private static readonly openedTrackHeight = metrics.closedTrackHeight * 4;
    static readonly brushSectionHeight = metrics.openedTrackHeight;       

    private static readonly itemHeight = 19;
    private static readonly itemMargin = 1;    
    private static readonly minItemWidth = 1;
   
    private static readonly minGapSize = 10 * 1000; // 10 seconds      
    private static readonly axisHeight = 35; 

    private static readonly indexesString = "Indexes";
    private static readonly documentsString = "Documents";
    private static readonly journalWriteString = "JournalWrite";
    private static readonly dataSyncString = "DataSync";
    private static readonly dataFlushString = "DataFlush";       

     /* observables */
       
    private importFileName = ko.observable<string>();   
    private isImport = ko.observable<boolean>(false);
    private hasIndexes = ko.observable<boolean>(false);
    private isIndexesExpanded = ko.observable<boolean>(false);
    private hasAnyData = ko.observable<boolean>(false);

    private searchText = ko.observable<string>();
    private trackNames = ko.observableArray<string>();
    private filteredIndexesTracksNames = ko.observableArray<string>();   

    private journalWriteLowSizeLevel = ko.observable<number>();
    private journalWriteHighSizeLevel = ko.observable<number>();
    private dataSyncLowSizeLevel = ko.observable<number>();
    private dataSyncHighSizeLevel = ko.observable<number>();
    private dataFlushLowSizeLevel = ko.observable<number>();
    private dataFlushHighSizeLevel = ko.observable<number>();

    /* private */

    private data: Raven.Server.Documents.Handlers.IOMetricsResponse;    
    private commonPathsPrefix: string;     
    private totalWidth: number;
    private totalHeight: number; 
    private currentYOffset = 0;
    private maxYOffset = 0;

    private gapFinder: gapFinder;   
    private hitTest = new hitTest();
    private brushSection: HTMLCanvasElement; // a virtual canvas for brush section
    private brushAndZoomCallbacksEnabled = true;    

    /* d3 */

    private isoParser = d3.time.format.iso;
    private xTickFormat = d3.time.format("%H:%M:%S");
    private canvas: d3.Selection<any>;
    private svg: d3.Selection<any>; // spans to canvas size (to provide brush + zoom/pan features)
    private brush: d3.svg.Brush<number>;
    private xBrushNumericScale: d3.scale.Linear<number, number>;
    private xBrushTimeScale: d3.time.Scale<number, number>;
    private xNumericScale: d3.scale.Linear<number, number>;   
    private brushContainer: d3.Selection<any>;
    private zoom: d3.behavior.Zoom<any>;
    private yScale: d3.scale.Ordinal<string, number>;
    private tooltip: d3.Selection<Raven.Server.Documents.Handlers.IOMetricsRecentStats | IndexingPerformanceGap>; 

    constructor() {
        super();
        this.searchText.throttle(200).subscribe(() => this.filterTracks());
    }

    activate(args: { indexName: string, database: string }): JQueryPromise<Raven.Server.Documents.Handlers.IOMetricsResponse> {
        super.activate(args);
        return this.getIOMetricsData();
    }

    compositionComplete() {
        super.compositionComplete();

        this.tooltip = d3.select(".tooltip");
        [this.totalWidth, this.totalHeight] = this.getPageHostDimenensions();
        this.totalHeight -= 50; // substract toolbar height

        this.initCanvas();

        this.hitTest.init(this.svg,
            () => this.onToggleIndexes(),
            (item, x, y) => this.handleTrackTooltip(item, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());
        
        this.initViewData();
        this.draw();
    }

    private initViewData() {
        let maxJournalWriteSize: number = 0;
        let maxVoronDataSyncSize: number = 0;
        let maxVoronDataFlushSize: number = 0;

        // 1. Find common paths prefix
        this.commonPathsPrefix = this.findPrefix(this.data.Environments.map(env => env.Path));

        // 2. Loop on info from EndPoint 
        this.data.Environments.forEach(env => {           

            // 2.0 Set the track name for the database path
            let trackName = env.Path.substring(this.commonPathsPrefix.length + 1);
            if (trackName === "") {
                env.Path = `${env.Path}\\${metrics.documentsString}`;
            }

            // 2.1 Check if indexes exist
            if (env.Path.substring(this.commonPathsPrefix.length + 1).startsWith(metrics.indexesString)) {              
                this.hasIndexes(true);
            } 

            // 2.2 Retrieve data for legend
            env.Files.forEach(file => {
                file.Recent.forEach(recentItem => {
                   
                    // 2.3 Calc highest batch size for each type
                    if (recentItem.Type === metrics.journalWriteString) {
                        maxJournalWriteSize = recentItem.Size > maxJournalWriteSize ? recentItem.Size : maxJournalWriteSize;
                    }
                    if (recentItem.Type === metrics.dataSyncString) {
                        maxVoronDataSyncSize = recentItem.Size > maxVoronDataSyncSize ? recentItem.Size : maxVoronDataSyncSize;
                    }
                    if (recentItem.Type === metrics.dataFlushString) {
                        maxVoronDataFlushSize = recentItem.Size > maxVoronDataFlushSize ? recentItem.Size : maxVoronDataFlushSize;
                    }

                    this.hasAnyData(true);
                });
            });
        });

        // 3. Calc levels so we know what color to use for the data in UI (low/med/high)
        this.journalWriteLowSizeLevel(generalUtils.roundBytesToNearstSize(maxJournalWriteSize / 3));
        this.journalWriteHighSizeLevel(generalUtils.roundBytesToNearstSize(maxJournalWriteSize / 3 * 2));

        this.dataSyncLowSizeLevel(generalUtils.roundBytesToNearstSize(maxVoronDataSyncSize / 3));
        this.dataSyncHighSizeLevel(generalUtils.roundBytesToNearstSize(maxVoronDataSyncSize / 3 * 2));

        this.dataFlushLowSizeLevel(generalUtils.roundBytesToNearstSize(maxVoronDataFlushSize / 3));
        this.dataFlushHighSizeLevel(generalUtils.roundBytesToNearstSize(maxVoronDataFlushSize / 3 * 2));
    }

    private initCanvas() {
        const metricsContainer = d3.select("#IOMetricsContainer");
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.svg = metricsContainer
            .append("svg")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.xBrushNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth - 1]) // substract 1px to avoid issue with missing right stroke
            .domain([0, this.totalWidth]);

        this.xNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth])
            .domain([0, this.totalWidth]);

        this.brush = d3.svg.brush()
            .x(this.xBrushNumericScale)
            .on("brush", () => this.onBrush());

        this.zoom = d3.behavior.zoom()
            .x(this.xNumericScale)
            .on("zoom", () => this.onZoom());

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight - metrics.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + metrics.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
        let mousePressed = false;

        const onMove = () => {
            this.hitTest.onMouseMove();
        }

        this.hitTest.cursor.subscribe((cursor) => {
            selection.style("cursor", cursor);
        });

        selection.on("mousemove.tip", onMove);

        selection.on("click", () => this.hitTest.onClick());

        selection
            .on("mousedown.tip", () => selection.on("mousemove.tip", null))
            .on("mouseup.tip", () => selection.on("mousemove.tip", onMove));

        selection
            .on("mousedown.yShift", () => {
                const node = selection.node();
                const initialClickLocation = d3.mouse(node);
                const initialOffset = this.currentYOffset;

                selection.on("mousemove.yShift", () => {
                    const currentMouseLocation = d3.mouse(node);
                    const yDiff = currentMouseLocation[1] - initialClickLocation[1];

                    const newYOffset = initialOffset - yDiff;

                    this.currentYOffset = newYOffset;
                    this.fixCurrentOffset();
                });

                selection.on("mouseup.yShift", () => selection.on("mousemove.yShift", null));
            });

        selection.on("dblclick.zoom", null);
    }

    private filterTracks() {
        const criteria = this.searchText().toLowerCase();                

        const indexesTracks = this.data.Environments.filter((x) => {
            let temp = x.Path.substring(this.commonPathsPrefix.length + 1);           
            return temp.startsWith(metrics.indexesString);
        });                 

        const indexesTracksNames = indexesTracks.map(x => x.Path.substring(this.commonPathsPrefix.length + 1 + metrics.indexesString.length + 1));

        // filteredIndexesTracksNames will be indexes tracks names that are NOT SUPPOSED TO BE SEEN ....
        this.filteredIndexesTracksNames(indexesTracksNames.filter(x => !(x.toLowerCase().includes(criteria))));       
                
        this.drawMainSection();
    }

    private draw() {
        if (this.hasAnyData()) {           

            // 0. Prepare
            this.prepareBrushSection();
            this.prepareMainSection();

            // 1. Draw the top brush section as image on the real DOM canvas
            const canvas = this.canvas.node() as HTMLCanvasElement;
            const context = canvas.getContext("2d");
            context.clearRect(0, 0, this.totalWidth, metrics.brushSectionHeight);
            context.drawImage(this.brushSection, 0, 0);

            // 2. Draw main (bottom) section
            this.drawMainSection();
        }        
    }

    private prepareBrushSection() {

        // 1. Prepare virtual canvas element for the brush section, will not be appended to the DOM
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth;
        this.brushSection.height = metrics.brushSectionHeight;
       
        const timeRanges = this.extractTimeRanges();
        this.gapFinder = new gapFinder(timeRanges, metrics.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        const context = this.brushSection.getContext("2d");
        this.drawXaxis(context, this.xBrushTimeScale, metrics.brushSectionHeight);

        context.strokeStyle = metrics.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth - 1, metrics.brushSectionHeight - 1);

        // 2. Draw accumulative data in the brush section (the top area)
        let yStartItem: number;
        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0);
        const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(xScale);

        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                file.Recent.forEach(recentItem => {

                    // TODO: Create algorithm to calculate the exact color to be painted in the brush section for the Accumulated Data,
                    //       Similar to what I did in indexing performance....  For now a default high color is used                       
                    context.fillStyle = this.calcItemColor(recentItem, false);

                    switch (recentItem.Type) {
                        case metrics.journalWriteString:
                            yStartItem = metrics.closedTrackHeight;
                            break;
                        case metrics.dataFlushString:
                            yStartItem = metrics.closedTrackHeight * 2;
                            break;
                        case metrics.dataSyncString:
                            yStartItem = metrics.closedTrackHeight * 3;       
                            break;                        
                    }
                   
                    // 3. Draw item in main canvas area 
                    const startDate = this.isoParser.parse(recentItem.Start);
                    const x1 = xScale(startDate);
                    let dx = extentFunc(recentItem.Duration);
                    dx = dx < metrics.minItemWidth ? metrics.minItemWidth : dx;
                    context.fillRect(x1, yStartItem, dx, metrics.trackHeight);                 
                });
            });
        });

        this.drawBrushGaps(context);
        this.prepareBrush();
    }

    private drawBrushGaps(context: CanvasRenderingContext2D) {
        context.strokeStyle = metrics.colors.gaps;

        for (let i = 0; i < this.gapFinder.gapsPositions.length; i++) {
            const gap = this.gapFinder.gapsPositions[i];
           
            const gapX = this.xBrushTimeScale(gap.start);
            context.moveTo(gapX, 1);
            context.lineTo(gapX, metrics.brushSectionHeight - 2);            
        }

        context.stroke();
    }

    private prepareBrush() {
        const hasBrush = !!this.svg.select("g.brush").node();

        if (!hasBrush) {
            this.brushContainer = this.svg
                .append("g")
                .attr("class", "x brush");

            this.brushContainer
                .call(this.brush)
                .selectAll("rect")
                .attr("y", 0)
                .attr("height", metrics.brushSectionHeight - 1);
        }
    }

    private prepareMainSection() {
        this.trackNames(this.findTrackNamesWithoutCommonPrefix());
        this.filteredIndexesTracksNames([]);       
    }

    private fixCurrentOffset() {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale() {              
        let currentOffset = metrics.axisHeight - this.currentYOffset;  
      
        let domain = [] as Array<string>;
        let range = [] as Array<number>;
        let firstIndex = true;        

        // TODO: Maybe refactor this method so it can handle any incoming number of environments,
        // But, as discussed, this will be left out for now inorder to avoid extra string comparisons

        // 1. Database main path
        domain.push(this.data.Environments[0].Path);
        range.push(currentOffset);
        currentOffset += metrics.openedTrackHeight + metrics.trackMargin;

        // 2. We want indexes to show in second track even though they are last in the endpoint info..
        if (this.hasIndexes()) {
            for (let i = 3; i < this.data.Environments.length; i++) {

                // 2.1 indexes closed
                if (!this.isIndexesExpanded()) {
                    if (firstIndex) {
                        domain.push(metrics.indexesString);
                        range.push(currentOffset);
                        firstIndex = false;
                    }
                    domain.push(this.data.Environments[i].Path);
                    range.push(currentOffset);
                }
                // 2.2 indexes opened
                else {
                    // If first index.... push the special indexes header ...
                    if (firstIndex) {
                        domain.push(metrics.indexesString);
                        range.push(currentOffset);
                        currentOffset += metrics.closedTrackHeight + metrics.trackMargin;
                        firstIndex = false;
                    }
                    // Push the index path - only if not filtered out..
                    if (!this.filtered(this.data.Environments[i].Path)) {
                        domain.push(this.data.Environments[i].Path);
                        range.push(currentOffset);
                        currentOffset += metrics.openedTrackHeight + metrics.trackMargin;
                    }
                }
            }

            if (!this.isIndexesExpanded()) {
                currentOffset += metrics.openedTrackHeight + metrics.trackMargin;
            }
        }        

        // 3. Subscription path
        domain.push(this.data.Environments[1].Path);
        range.push(currentOffset);
        currentOffset += metrics.openedTrackHeight + metrics.trackMargin;

        // 4. Configuration path
        domain.push(this.data.Environments[2].Path);
        range.push(currentOffset);
       
        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset() {    
        let offset = metrics.axisHeight;       

        if (this.isIndexesExpanded()) {
            offset += metrics.openedTrackHeight * this.data.Environments.length + metrics.closedTrackHeight;
        }
        else {                      
            offset += metrics.openedTrackHeight * 4; // * 4 because I have 4 tracks: Data|Indexes|Subscriptions|Configurations
        }        

        const extraBottomMargin = 100;
        const availableHeightForTracks = this.totalHeight - metrics.brushSectionHeight;        

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private findTrackNamesWithoutCommonPrefix(): string[] {
        const result = new Set<string>();              

        this.data.Environments.forEach(track => {
            let trackName = track.Path.substring(this.commonPathsPrefix.length + 1);           
            result.add(trackName);
        });

        return Array.from(result);
    }

    private drawXaxis(context: CanvasRenderingContext2D, scale: d3.time.Scale<number, number>, height: number) {
        try {
            context.save();
           
            const step = 200;
            const initialOffset = 100;

            const ticks = d3.range(initialOffset, this.totalWidth - step, step)
                .map(y => scale.invert(y));

            context.strokeStyle = metrics.colors.axis;
            context.fillStyle = metrics.colors.axis;

            // 1. Draw vertical dotted lines
            context.beginPath();
            context.setLineDash([4, 2]);
            ticks.forEach((x, i) => {
                context.moveTo(initialOffset + (i * step) + 0.5, 0);
                context.lineTo(initialOffset + (i * step) + 0.5, height);
            });
            context.stroke();

            // 2. Draw the time
            context.beginPath();
            context.textAlign = "left";
            context.textBaseline = "top";
            context.font = "10px Lato";
            ticks.forEach((x, i) => {               
                context.fillText(this.xTickFormat(x), initialOffset + (i * step) + 5, 5); // 5px left padding
            });
        }
        finally {
            context.restore();
        }             
    }

    private onZoom() {
        if (this.brushAndZoomCallbacksEnabled) {
            this.brush.extent(this.xNumericScale.domain() as [number, number]);
            this.brushContainer
                .call(this.brush);

            this.drawMainSection();
        }
    }

    private onBrush() {
        if (this.brushAndZoomCallbacksEnabled) {
            this.xNumericScale.domain((this.brush.empty() ? this.xBrushNumericScale.domain() : this.brush.extent()) as [number, number]);
            this.zoom.x(this.xNumericScale);
            this.drawMainSection();
        }
    }

    private drawMainSection() {
        this.hitTest.reset();
        this.calcMaxYOffset();
        this.fixCurrentOffset();
        this.constructYScale();

        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0);
        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        let yStartItem: number;
        let firstIndexTrack = true;

        try {
            context.save();

            context.translate(0, metrics.brushSectionHeight); 
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - metrics.brushSectionHeight);                                                   
           
            context.rect(0, 0, this.totalWidth, this.totalHeight - metrics.brushSectionHeight);
            context.clip();

            // 1. Draw tracks background 
            this.drawTracksBackground(context, xScale);           

            // 2. Draw the vertical dotted lines                  
            if (xScale.domain().length) {
                this.drawXaxis(context, xScale, this.totalHeight);
            }          

            // 3. Draw all other data (track name + items on track)                                 
            context.beginPath();        
            context.rect(0, metrics.axisHeight, this.totalWidth, this.totalHeight - metrics.brushSectionHeight);
            context.clip(); 
            
            const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(xScale);

            this.data.Environments.forEach(env => {

                // 3.1. Check if this is an index track 
                let trackName = env.Path.substring(this.commonPathsPrefix.length + 1);               
                let isIndexTrack = trackName.startsWith(metrics.indexesString) ? true : false;                                 
               
                // 3.2 Draw track name   
                const yStart = this.yScale(env.Path);                                                                                 
                this.drawTrackName(context, trackName, yStart);               
               
                env.Files.forEach(file => {
                    file.Recent.forEach(recentItem => {                                                              
                        if (!this.filtered(env.Path)) {

                            // 3.3 Determine color for item                       
                            let calcColorBasedOnSize = true;
                            if (!this.isIndexesExpanded() && isIndexTrack) { 
                                                        
                                // TODO: create algorithm to calculate the exact color to be painted - same TODO as in the brush section...
                                calcColorBasedOnSize = false;
                            }
                            context.fillStyle = this.calcItemColor(recentItem, calcColorBasedOnSize);

                            // 3.4 Determine yStart for item
                            switch (recentItem.Type) {
                                case metrics.journalWriteString: yStartItem = yStart + metrics.closedTrackHeight + metrics.itemMargin; break;                             
                                case metrics.dataFlushString: yStartItem = yStart + metrics.closedTrackHeight + metrics.itemMargin * 2 + metrics.itemHeight; break;
                                case metrics.dataSyncString: yStartItem = yStart + metrics.closedTrackHeight + metrics.itemMargin * 3 + metrics.itemHeight * 2; break;
                            }

                            // 3.5 Draw item in main canvas area 
                            const startDate = this.isoParser.parse(recentItem.Start);
                            const x1 = xScale(startDate);
                            let dx = extentFunc(recentItem.Duration);
                            dx = dx < metrics.minItemWidth ? metrics.minItemWidth : dx;
                            context.fillRect(x1, yStartItem, dx, metrics.itemHeight);

                            // 3.6 Draw the human size text on the item if there is enough space.. but don't draw on closed indexes track 
                            // Logic: Don't draw if: [closed && isIndexTrack] ==> [!(closed && isIndexTrack)] ==> [open || !isIndexTrack]
                            if (this.isIndexesExpanded() || !isIndexTrack) {
                                const humanSizeTextWidth = context.measureText(recentItem.HumanSize).width;
                                if (dx > humanSizeTextWidth) {
                                    context.fillStyle = 'black';
                                    context.fillText(recentItem.HumanSize, x1 + dx / 2 - humanSizeTextWidth / 2, yStartItem + metrics.trackHeight / 2 + 4);
                                }

                                // 3.7 Register track item for tooltip (but not for the 'closed' indexes track)
                                this.hitTest.registerTrackItem(x1 - 2, yStartItem, dx + 2, metrics.itemHeight, recentItem);
                            }    

                            // 3.8 If on the closed index track, might as well register toggle, so that indexes details will open, can be nice 
                            if (!this.isIndexesExpanded() && isIndexTrack) {
                                this.hitTest.registerIndexToggle(x1 - 5, yStartItem, dx + 5, metrics.itemHeight);

                            }
                        }
                    });
                });
            });
                        
            this.drawGaps(context, xScale);            
        }
        finally {
            context.restore();
        }
    }

    private filtered(envPath: string): boolean {

        return _.includes(this.filteredIndexesTracksNames(), envPath.substring(this.commonPathsPrefix.length + 1 + metrics.indexesString.length + 1));        
    }

    private drawTracksBackground(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        context.save();
        context.beginPath();

        context.rect(0, metrics.axisHeight, this.totalWidth, this.totalHeight - metrics.brushSectionHeight);
        context.clip();

        context.fillStyle = metrics.colors.trackBackground;
        this.data.Environments.forEach(env => {  
             if (!this.filtered(env.Path)) {
                - context.fillRect(0, this.yScale(env.Path), this.totalWidth, metrics.openedTrackHeight);
             }               
        });

        // The special case...draw the additional index heading when in expanded state
        if (this.isIndexesExpanded()) {
            context.fillRect(0, this.yScale(metrics.indexesString), context.measureText(metrics.indexesString).width + 30, metrics.closedTrackHeight);
            this.drawTrackName(context, metrics.indexesString, this.yScale(metrics.indexesString));
        }

        context.closePath();
        context.restore();
    }

    private drawTrackName(context: CanvasRenderingContext2D, trackName: string, yStart: number) {
        const yTextShift = 14.5;
        const xTextShift = 0.5;
        let xTextStart = 5;
        let rectWidth;             
        let addedWidth = 8;
        let drawArrow = false;       

        const isIndexTrack = trackName.startsWith(metrics.indexesString);

        // 1. Draw background color for track name - first check if track is an 'index' track
        if (isIndexTrack) {
            xTextStart = 15;
            addedWidth = 18;

            trackName = trackName.substring(metrics.indexesString.length + 1);

            // 1.1 The first indexes track has the track name of: 'Indexes' (both when opened or closed..)
            if ((trackName === "") || (!this.isIndexesExpanded())) {
                trackName = metrics.indexesString;                
                addedWidth = 23;
                drawArrow = true;                       
            }           
        }     

        context.font = "12px Lato"; // Define font before using measureText()...
        rectWidth = context.measureText(trackName).width + addedWidth;
        context.fillStyle = metrics.colors.trackNameBg;

        if (!_.includes(this.filteredIndexesTracksNames(), trackName)) {
            context.fillRect(2, yStart + metrics.closedTrackPadding, rectWidth, metrics.trackHeight);
        }

        // 2. Draw arrow only for indexes track
        if (drawArrow) {                 
            context.fillStyle = this.isIndexesExpanded() ? metrics.colors.openedTrackArrow : metrics.colors.closedTrackArrow;
            graphHelper.drawArrow(context, 5, yStart + 6, !this.isIndexesExpanded());            
            this.hitTest.registerIndexToggle(2, yStart + metrics.closedTrackPadding, rectWidth, metrics.trackHeight);
        }

        // 3. Draw track name (if not filtered out..)                
        context.fillStyle = metrics.colors.trackNameFg;
                         
        if (!_.includes(this.filteredIndexesTracksNames(), trackName)) {
            context.fillText(trackName, xTextStart + xTextShift, yStart + yTextShift);
        }
    }

    private drawGaps(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {

        const range = xScale.range();
        context.strokeStyle = metrics.colors.gaps;

        for (let i = 1; i < range.length; i += 2) {
            const gapX = Math.floor(range[i]) + 0.5;

            context.beginPath();
            context.moveTo(gapX, metrics.axisHeight);
            context.lineTo(gapX, this.totalHeight);
            context.stroke();

            const indexToGapFinderPosition = (i - 1) / 2;
            const gapInfo = this.gapFinder.gapsPositions[indexToGapFinderPosition];
            if (gapInfo) {
                // Register gap for tooltip 
                this.hitTest.registerGapItem(gapX - 5, metrics.axisHeight, 10, this.totalHeight,
                    { DurationInMilliseconds: gapInfo.durationInMillis, StartTime: gapInfo.start.toLocaleTimeString() });
            }
        }
    }

    private onToggleIndexes() {
        this.isIndexesExpanded.toggle();
        this.drawMainSection();
    }

    expandIndexes() {  
        this.isIndexesExpanded(true);
        this.drawMainSection();
    }

    collapseIndexes() {        
        this.isIndexesExpanded(false);
        this.drawMainSection();
    }   

    private getIOMetricsData(): JQueryPromise<Raven.Server.Documents.Handlers.IOMetricsResponse> {
        return new getIOMetricsCommand(this.activeDatabase())
            .execute()
            .done((result) => {
                this.data = result;
            });
    }

    fileSelected() {
        const fileInput = <HTMLInputElement>document.querySelector("#importFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function () {
            // ReSharper disable once SuspiciousThisUsage
            self.dataImported(this.result);
        };
        reader.onerror = function (error: any) {
            alert(error);
        };
        reader.readAsText(file);

        this.importFileName(fileInput.files[0].name);

        // Must clear the filePicker element value so that user will be able to import the -same- file after closing the imported view...
        let $input = $("#importFilePicker");
        $input.val(null);
    }

    private dataImported(result: string) {              
        try {
            const importedData: Raven.Server.Documents.Handlers.IOMetricsResponse = JSON.parse(result); 

            // Check if data is an IOStats json data..                                  
            if (!importedData.hasOwnProperty('Environments')) {
                messagePublisher.reportError("Invalid IO Stats file format", undefined, undefined);
            }
            else {
                this.data = importedData;
                this.resetGraphData();
                this.draw();
                this.isImport(true);
            }

        } catch (e) {
            messagePublisher.reportError("Failed to parse json data", undefined, undefined);
        }             
    }

    closeImport() {       
        this.getIOMetricsData().done(() => {                                                                   
            
            this.resetGraphData();
            this.draw();            
            this.isImport(false);
        });
    }   

    private resetGraphData() {
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());      
        this.initViewData(); 
        this.searchText("");
    }

    private setZoomAndBrush(scale: [number, number], brushAction: (brush: d3.svg.Brush<any>) => void) {
        this.brushAndZoomCallbacksEnabled = false;

        this.xNumericScale.domain(scale);
        this.zoom.x(this.xNumericScale);

        brushAction(this.brush);
        this.brushContainer.call(this.brush);

        this.brushAndZoomCallbacksEnabled = true;
    }

    exportAsJson() {
        let exportFileName;

        if (this.isImport()) {
            exportFileName = this.importFileName().substring(0, this.importFileName().lastIndexOf('.'));
        }
        else {
            exportFileName = `IOStats-of-${this.activeDatabase().name}-${moment().format("YYYY-MM-DD-HH-mm")}`;
        }

        fileDownloader.downloadAsJson(this.data, exportFileName + ".json", exportFileName);
    }

    private findPrefix(strings: Array<string>) {
        if (!strings.length) {
            return "";
        }

        const sorted = strings.slice(0).sort();
        const string1 = sorted[0];
        const string2 = sorted[sorted.length - 1];
        let i = 0;
        const l = Math.min(string1.length, string2.length);

        while (i < l && string1[i] === string2[i]) {
            i++;
        }

        return string1.slice(0, i);
    }

    private calcItemColor(recentItem: Raven.Server.Documents.Handlers.IOMetricsRecentStats, calcColorBasedOnSize: boolean): string {
        let color: string;

        switch (recentItem.Type) {
            case metrics.journalWriteString: {
                color = metrics.eventsColors.JournalWriteLowSizeColor;
                if (recentItem.Size > this.journalWriteLowSizeLevel()) {
                    color = metrics.eventsColors.JournalWriteMedSizeColor;
                }
                if (recentItem.Size > this.journalWriteHighSizeLevel()) {
                    color = metrics.eventsColors.JournalWriteHighSizeColor;
                }
                if (!calcColorBasedOnSize) {
                    color = metrics.eventsColors.JournalWriteHighSizeColor;
                }
            } break;
            case metrics.dataSyncString: {
                color = metrics.eventsColors.DataSyncLowSizeColor;
                if (recentItem.Size > this.dataSyncLowSizeLevel()) {
                    color = metrics.eventsColors.DataSyncMedSizeColor;
                }
                if (recentItem.Size > this.dataSyncHighSizeLevel()) {
                    color = metrics.eventsColors.DataSyncHighSizeColor;
                }
                if (!calcColorBasedOnSize) {
                    color = metrics.eventsColors.DataSyncHighSizeColor;
                }
            } break;
            case metrics.dataFlushString: {
                color = metrics.eventsColors.DataFlushLowSizeColor;
                if (recentItem.Size > this.dataFlushLowSizeLevel()) {
                    color = metrics.eventsColors.DataFlushMedSizeColor;
                }
                if (recentItem.Size > this.dataFlushHighSizeLevel()) {
                    color = metrics.eventsColors.DataFlushHighSizeColor;
                }
                if (!calcColorBasedOnSize) {
                    color = metrics.eventsColors.DataFlushHighSizeColor;
                }
            } break;
        }

        return color;
    }

    private extractTimeRanges(): Array<[Date, Date]>{        
        const result = [] as Array<[Date, Date]>;
        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                file.Recent.forEach(recentItem => {
                    // Get the events time ranges
                    const startTime = this.isoParser.parse(recentItem.Start);
                    const endTime = new Date(startTime.getTime() + recentItem.Duration);
                    result.push([startTime, endTime]);                   
                });
            });
        });  

        return result;      
    }

    private computedHumanSize(input: KnockoutObservable<number>): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            return generalUtils.formatBytesToSize(input());
        });
    }

    /*
    * The following methods are called by hitTest class on mouse move    
    */

    private handleGapTooltip(element: IndexingPerformanceGap, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            const tooltipHtml = "Gap start time: " + (element).StartTime +
                "<br/>Gap duration: " + generalUtils.formatMillis((element).DurationInMilliseconds);
            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleTrackTooltip(element: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) {     
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            let typeString;
            switch (element.Type) {
                case "JournalWrite": typeString = "Journal Write"; break;
                case "DataSync": typeString = "Voron Data Sync"; break;
                case "DataFlush": typeString = "Voron Data Flush"; break;
            }
            let tooltipHtml = `*** ${typeString} ***<br/>`;
            let duration = (element.Duration === 0) ? "0" : generalUtils.formatMillis((element).Duration);
            tooltipHtml += `Duration: ${duration}<br/>`;
            tooltipHtml += `Size: ${element.HumanSize}<br/>`;
            tooltipHtml += `Size (bytes): ${element.Size.toLocaleString()}<br/>`;
            tooltipHtml += `Allocated Size: ${element.HumanFileSize.toLocaleString()}<br/>`;
            tooltipHtml += `Allocated Size (bytes): ${element.FileSize.toLocaleString()}<br/>`;

            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleTooltip(element: Raven.Server.Documents.Handlers.IOMetricsRecentStats | IndexingPerformanceGap, x: number, y: number, tooltipHtml: string) {
        if (element) {         
            const canvas = this.canvas.node() as HTMLCanvasElement;
            const context = canvas.getContext("2d");
            context.font = this.tooltip.style("font"); 
          
            const longestLine = generalUtils.findLongestLineInTooltip(tooltipHtml);               
            const tooltipWidth = context.measureText(longestLine).width + 60;
          
            const numberOfLines = generalUtils.findNumberOfLinesInTooltip(tooltipHtml);
            const tooltipHeight = numberOfLines * 30 + 60;      

            x = Math.min(x, Math.max(this.totalWidth - tooltipWidth, 0));
            y = Math.min(y, Math.max(this.totalHeight - tooltipHeight, 0));

            this.tooltip                           
                .style("left", (x + 10) + "px")
                .style("top", (y + 10) + "px");    
           
            this.tooltip
                .transition()
                .duration(250)
                .style("opacity", 1);    

            this.tooltip
                .html(tooltipHtml)
                .datum(element);

        } else {
            this.hideTooltip();
        }
    }
    private hideTooltip() {       
        this.tooltip
            .transition()
            .duration(250)
            .style("opacity", 0);

        this.tooltip.datum(null);
    }   
}

export = metrics;

