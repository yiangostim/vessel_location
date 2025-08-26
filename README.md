# vessel_location
Vessel location for 40.000-100.000 DWT bulk carriers

navigation_status
0 = under way using engine
1 = at anchor
2 = not under command
3 = restricted maneuverability
4 = constrained by her draught
5 = moored
6 = aground
7 = engaged in fishing
8 = under way sailing
9 = reserved for future amendment of navigational status for ships carrying DG, HS, or MP, or IMO hazard or pollutant category C, high-speed craft (HSC)
10 = reserved for future amendment of navigational status for ships carrying dangerous goods (DG), harmful substances (HS) or marine pollutants (MP), or IMO hazard or pollutant category A, wing in ground (WIG)
11 = power-driven vessel towing astern (regional use)
12 = power-driven vessel pushing ahead or towing alongside (regional use)
13 = reserved for future use
14 = AIS-SART (active), MOB-AIS, EPIRB-AIS
15 = undefined = default (also used by AIS-SART, MOB-AIS and EPIRB-AIS under test)

Note that, if you are using MarineTraffic API Services, it is possible to get STATUS responses such as the following ones (not AIS-derived):

95 = Base Station
​96 = Class B
​97 = SAR Aircraft
​98 = Aid to Navigation
​99 = Class B



ship_type

70	Cargo, all ships of this type
71	Cargo, Hazardous category A
72	Cargo, Hazardous category B
73	Cargo, Hazardous category C
74	Cargo, Hazardous category D
75	Cargo, Reserved for future use
76	Cargo, Reserved for future use
77	Cargo, Reserved for future use
78	Cargo, Reserved for future use
79	Cargo, No additional information
