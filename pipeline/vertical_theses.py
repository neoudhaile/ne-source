"""
Vertical-specific thesis statements and context for email generation.

Each key is a vertical name (lowercased). The email generator fuzzy-matches
the lead's industry field against these keys.

Structure per entry:
  thesis  — why Broeren & Co. is focused on this vertical (inserted into email)
  aliases — alternate industry names that should map to this vertical
"""

VERTICAL_THESES = {
    "auto service": {
        "aliases": [
            "auto repair", "general mechanical", "auto mechanic",
            "car repair", "automotive repair", "automotive service",
        ],
        "thesis": (
            "cars aren't going anywhere, and neither is the need for "
            "independent shops that actually know how to fix them \u2014 the "
            "dealerships are expensive, the chains are impersonal, and the "
            "customers who find a shop they trust don't leave.\n\n"
            "At the same time, the average age of vehicles on the road in the "
            "US just hit an all-time high \u2014 people are holding onto their cars "
            "longer, which means more repair demand, not less, and independent "
            "shops that have earned their reputation are positioned better than ever."
        ),
    },
    "fleet maintenance": {
        "aliases": [
            "fleet service", "fleet repair", "fleet management",
        ],
        "thesis": (
            "commercial fleets can't afford downtime, and the operators who "
            "service them build relationships that are essentially locked in "
            "\u2014 a fleet manager who trusts you with 40 vehicles isn't shopping "
            "around every year.\n\n"
            "The broader trend is working in this direction too \u2014 last-mile "
            "delivery, construction activity, and municipal fleet expansion "
            "across Southern California have all grown the pool of vehicles "
            "that need dedicated service, and the independent shops with the "
            "capacity and the relationships to handle it are in short supply."
        ),
    },
    "brake and alignment": {
        "aliases": [
            "brake repair", "alignment", "tune-up", "engine repair",
            "tune up", "brake service", "engine service",
        ],
        "thesis": (
            "the mechanical repair market in Southern California is enormous "
            "and the independent shops doing real diagnostic and repair work "
            "\u2014 not just oil changes \u2014 serve a customer base that keeps coming "
            "back because they can't afford to go anywhere else.\n\n"
            "With EVs still a small fraction of the vehicle population and "
            "internal combustion vehicles lasting longer than ever, the demand "
            "runway for skilled mechanical repair shops in this region is "
            "longer than most people assume."
        ),
    },
    "diesel repair": {
        "aliases": [
            "diesel service", "diesel mechanic", "diesel shop",
            "truck repair", "heavy duty repair",
        ],
        "thesis": (
            "diesel repair is a specialized skill that most general shops "
            "won't touch, and the trucking, construction, and agricultural "
            "operators who depend on it need a shop they can count on \u2014 that "
            "loyalty is hard to displace once it's earned.\n\n"
            "With the Port of Los Angeles, the Inland Empire logistics "
            "corridor, and California's construction activity all driving "
            "diesel-powered equipment demand, the shops that have built real "
            "diesel capability in Southern California are serving a market "
            "that isn't slowing down."
        ),
    },
    "smog station": {
        "aliases": [
            "smog check", "smog test", "emissions testing",
            "smog certification",
        ],
        "thesis": (
            "California's smog compliance requirements aren't going away, and "
            "stations that have built a steady customer base in their zip code "
            "operate with a level of recurring, non-discretionary demand that "
            "most service businesses would envy.\n\n"
            "If anything the regulatory environment in California is "
            "tightening, not loosening \u2014 which means the compliance side of "
            "this business has a structural tailwind that's independent of "
            "what the broader economy is doing."
        ),
    },
    "calibration": {
        "aliases": [
            "metrology", "instrument calibration", "gauge calibration",
            "NDT", "dimensional inspection", "nondestructive testing",
        ],
        "thesis": (
            "calibration and metrology services sit at the intersection of "
            "regulatory compliance and operational necessity \u2014 the "
            "manufacturers, aerospace suppliers, and defense contractors who "
            "need certified instruments calibrated on schedule don't have the "
            "option to skip it, and they need a provider they can trust to "
            "get it right.\n\n"
            "With aerospace and defense activity concentrated heavily in "
            "Southern California and domestic manufacturing investment "
            "accelerating off the back of reshoring trends, the demand for "
            "accredited calibration and inspection services in this region is "
            "growing faster than the supply of qualified providers."
        ),
    },
    "pump and motor": {
        "aliases": [
            "electric motor repair", "motor rewind", "pump rebuilding",
            "hydraulic repair", "rotating equipment", "pump repair",
            "motor repair",
        ],
        "thesis": (
            "rotating equipment is the backbone of manufacturing, water "
            "infrastructure, and industrial processing \u2014 when a motor or pump "
            "goes down, the customer needs it back fast, and the shops with "
            "the rewind capability and rebuild expertise to handle it are a "
            "shrinking group.\n\n"
            "Across the country, the motor rewind and rotating equipment "
            "repair industry has been consolidating for years \u2014 independent "
            "shops are closing faster than new ones are opening, which means "
            "the ones still standing are absorbing more demand and carrying "
            "more value than their size might suggest."
        ),
    },
    "compressor": {
        "aliases": [
            "compressed air", "air compressor service",
            "compressed air systems", "pneumatic systems",
            "compressor repair",
        ],
        "thesis": (
            "compressed air is the fourth utility in most manufacturing and "
            "industrial facilities \u2014 it touches everything, it can't go down, "
            "and the service providers who've built relationships with plant "
            "managers and maintenance teams have accounts that renew year "
            "after year without much selling required.\n\n"
            "As domestic manufacturing investment accelerates and industrial "
            "facilities across Southern California expand or reopen, the "
            "demand for reliable compressed air service is growing alongside "
            "it \u2014 and the established independents with the customer base and "
            "the technical depth are the ones positioned to capture it."
        ),
    },
    "equipment maintenance": {
        "aliases": [
            "mechanical contracting", "industrial repair contractor",
            "field service contractor", "preventive maintenance", "MRO",
            "industrial maintenance",
        ],
        "thesis": (
            "industrial facilities have more equipment and fewer internal "
            "maintenance staff than they did a decade ago \u2014 the contractors "
            "who've built the field service capability and the customer "
            "relationships to fill that gap are doing work that's genuinely "
            "difficult to replace with a new vendor.\n\n"
            "The broader shift toward outsourced maintenance \u2014 driven by labor "
            "costs, liability, and the difficulty of finding qualified "
            "in-house technicians \u2014 has been accelerating for years and shows "
            "no signs of reversing, which means the field service contractors "
            "who've already established themselves are sitting on a growing "
            "book of business."
        ),
    },
    "hydraulic hose": {
        "aliases": [
            "hydraulic hose and fittings", "hydraulic fittings",
            "hose shop", "hydraulic service",
        ],
        "thesis": (
            "hydraulic hose shops serve contractors, equipment operators, and "
            "industrial customers who need a fitting made or a hose replaced "
            "right now \u2014 the walk-in urgency of the business creates customer "
            "loyalty that's less about price and more about who can solve the "
            "problem today.\n\n"
            "With construction activity across Southern California at "
            "sustained highs and the equipment population growing alongside "
            "it, the demand for fast, reliable hydraulic service has never "
            "been higher \u2014 and the independent shops that have built their "
            "reputation on turnaround time are the ones customers call first."
        ),
    },
    "conveyor system": {
        "aliases": [
            "conveyor repair", "conveyor installation",
            "material handling", "conveyor service",
        ],
        "thesis": (
            "the logistics and distribution build-out across Southern "
            "California has driven real demand for conveyor installation and "
            "service, and the operators who've built field service depth and "
            "long-term customer relationships in this market have a position "
            "that's genuinely hard to displace.\n\n"
            "The Inland Empire is now one of the largest logistics hubs in "
            "the country \u2014 the warehouse and distribution infrastructure "
            "that's been built there over the last decade runs on conveyor "
            "and material handling systems that need ongoing installation, "
            "maintenance, and repair, and the qualified independents in that "
            "ecosystem are in a strong position for the long term."
        ),
    },
    "welding and fabrication": {
        "aliases": [
            "welding", "fabrication", "custom fabrication",
            "metal fabrication", "welding shop",
        ],
        "thesis": (
            "custom fabrication shops serve a customer base that needs things "
            "built to spec and built right \u2014 once a manufacturer or contractor "
            "finds a shop they trust to hold tolerances and meet deadlines, "
            "they don't go looking for another one.\n\n"
            "Reshoring of domestic manufacturing and increased infrastructure "
            "spending are both driving demand for custom fabrication capacity "
            "in the US \u2014 and the established independent shops with the "
            "equipment, the skilled welders, and the customer relationships "
            "are better positioned to capture that demand than anyone starting "
            "from scratch."
        ),
    },
    "industrial valve": {
        "aliases": [
            "valve repair", "valve testing", "valve service",
            "industrial valve repair",
        ],
        "thesis": (
            "valve repair and testing is a niche that sits at the center of "
            "refining, petrochemical, water, and power generation operations "
            "\u2014 the facilities that depend on it operate under regulatory and "
            "safety requirements that make reliability non-negotiable, and the "
            "shops certified to do the work have a defensible position most "
            "competitors can't easily enter.\n\n"
            "California's aging water and wastewater infrastructure is under "
            "significant pressure to upgrade and maintain \u2014 the federal "
            "infrastructure bill has directed billions toward exactly this, "
            "and the valve repair and testing shops that serve municipal and "
            "utility customers are in the middle of a spending cycle that has "
            "years left to run."
        ),
    },
    "gearbox repair": {
        "aliases": [
            "gearhead repair", "gearbox service", "gear repair",
            "drivetrain repair",
        ],
        "thesis": (
            "gearbox repair requires a level of precision and application "
            "knowledge that most general machine shops don't have \u2014 the "
            "industrial and manufacturing customers who've found a shop they "
            "trust with their drivetrain equipment treat it as a long-term "
            "relationship, not a transaction.\n\n"
            "As the domestic manufacturing base rebuilds and industrial "
            "equipment ages without being replaced, the repair and rebuild "
            "side of the gearbox market is growing \u2014 it's cheaper to overhaul "
            "a gearbox than to replace it, and the shops with the expertise "
            "to do it right are capturing demand that used to go to OEM "
            "replacement."
        ),
    },
    "generator service": {
        "aliases": [
            "generator repair", "generator maintenance",
            "backup power", "standby generator",
        ],
        "thesis": (
            "generators are the last line of defense for hospitals, data "
            "centers, municipalities, and industrial facilities \u2014 the service "
            "providers who've built the expertise and the customer "
            "relationships in this space are doing mission-critical work that "
            "customers aren't willing to hand to an unknown vendor.\n\n"
            "With California's grid reliability challenges accelerating "
            "investment in backup power across both commercial and industrial "
            "sectors, the demand for qualified generator service and "
            "maintenance has been growing steadily \u2014 and the established "
            "independents with certified technicians and a proven customer "
            "base are the ones capturing that growth."
        ),
    },
    "forklift repair": {
        "aliases": [
            "forklift service", "forklift maintenance",
            "lift truck repair", "material handling equipment",
        ],
        "thesis": (
            "warehouses and distribution centers can't operate without their "
            "lift equipment, and the independent forklift service shops that "
            "have built recurring maintenance contracts with their customers "
            "have some of the most predictable, sticky revenue in the "
            "equipment service space.\n\n"
            "The explosion of warehouse and fulfillment infrastructure across "
            "the Inland Empire and greater Southern California has added tens "
            "of thousands of forklifts to the regional equipment population "
            "over the last decade \u2014 the service demand that comes with that "
            "growth is real, recurring, and concentrated in exactly the "
            "geography where established independents already operate."
        ),
    },
    "cnc machine repair": {
        "aliases": [
            "CNC repair", "CNC service", "CNC maintenance",
            "machine tool repair", "CNC machine service",
        ],
        "thesis": (
            "CNC machine repair is one of the most technically demanding "
            "niches in industrial service \u2014 the manufacturers who depend on "
            "this equipment for production can't afford extended downtime, and "
            "the technicians who can diagnose and fix it correctly are "
            "genuinely rare, which means the shops that have built this "
            "capability have a real moat.\n\n"
            "As domestic aerospace, defense, and precision manufacturing "
            "activity in Southern California grows and the installed base of "
            "CNC equipment ages, the repair and retrofit side of the market "
            "is expanding \u2014 OEM support for older machines gets harder to find "
            "every year, which pushes more demand toward the independent shops "
            "that have built the expertise to handle it."
        ),
    },
}


def get_thesis(industry: str) -> str | None:
    """
    Match a lead's industry string to a vertical thesis.
    Returns the thesis text or None if no match.

    Matching order:
    1. Direct key match (lowercased)
    2. Alias match (any alias contained in the industry string, or vice versa)
    """
    if not industry:
        return None

    ind_lower = industry.lower().strip()

    # Direct key match
    if ind_lower in VERTICAL_THESES:
        return VERTICAL_THESES[ind_lower]["thesis"]

    # Check if industry string contains a key or vice versa
    for key, entry in VERTICAL_THESES.items():
        if key in ind_lower or ind_lower in key:
            return entry["thesis"]
        for alias in entry["aliases"]:
            if alias in ind_lower or ind_lower in alias:
                return entry["thesis"]

    return None
