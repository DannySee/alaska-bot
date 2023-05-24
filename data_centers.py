import pyodbc

# sql server connection properties
sql_server = pyodbc.connect(
    driver='{SQL Server}',
    server='MS248CSSQL01.SYSCO.NET',
    database='Pricing_Agreements')

# dictionary of all opsites 3 (key=3 digit number, value=3 digit number + name)
site_names = {
    '001': '001 - Jackson',
    '002': '002 - Atlanta',
    '003': '003 - Jacksonville',
    '004': '004 - Central California (Modesto)',
    '005': '005 - Intermountain (UT)',
    '006': '006 - Dallas (North Texas)',
    '007': '007 - Virginia',
    '008': '008 - Northern New England',
    '009': '009 - Pittsburgh',
    '010': '010 - East Maryland (Lankford)',
    '011': '011 - Lousiville',
    '012': '012 - Baltimore (Smelkinson)',
    '013': '013 - Central TX (San Antonio & Austin)',
    '014': '014 - Memphis (Hardins)',
    '015': '015 - Cleveland',
    '016': '016 - South Florida (Miami)(Medley)',
    '017': '017 - Las Vegas',
    '018': '018 - Baraboo (Winsconsin)',
    '019': '019 - Cincinnati',
    '022': '022 - Central Florida (Ocoee)',
    '023': '023 - New Orleans',
    '024': '024 - Chicago',
    '025': '025 - Albany',
    '026': '026 - Oklahoma',
    '027': '027 - Syracuse',
    '029': '029 - Arkansas',
    '031': '031 - Sacramento',
    '032': '032 - S.E. Florida',
    '035': '035 - East Wisconsin',
    '036': '036 - San Diego',
    '037': '037 - West Coast Florida (Palmetto)',
    '038': '038 - Indianapolis',
    '039': '039 - Iowa',
    '040': '040 - Idaho',
    '043': '043 - Montana',
    '045': '045 - Los Angeles',
    '046': '046 - Central Alabama (Birmingham)',
    '047': '047 - Minnesota',
    '048': '048 - Charlotte',
    '049': '049 - Arizona',
    '050': '050 - San Francisco',
    '051': '051 - Central PA',
    '052': '052 - Portland',
    '054': '054 - Connecticut',
    '055': '055 - Seattle (Washington)',
    '056': '056 - Boston (Hallsmith)',
    '057': '057 - Kansas City',
    '058': '058 - Detroit',
    '059': '059 - Denver',
    '060': '060 - Nashville (Robert Orr)',
    '061': '061 - Nebraska (Lincoln)(Pegler) ',
    '064': '064 - St. Louis',
    '066': '066 - New Mexico  (Nobel) ',
    '067': '067 - Houston',
    '068': '068 - Grand Rapids',
    '073': '073 - Hampton Roads (VA)',
    '075': '075 - Philadelphia',
    '076': '076 - Metro New York',
    '078': '078 - West Texas (Watson)(Lubbock)',
    '101': '101 - Ventura',
    '102': '102 - Spokane',
    '134': '134 - Intl Food Group (IFG)-Tampa',
    '137': '137 - Columbia',
    '163': '163 - Raleigh',
    '164': '164 - Gulf Coast',
    '194': '194 - Central Illinois (Roberts)',
    '195': '195 - North Dakota',
    '288': '288 - Knoxville',
    '293': '293 - East Texas',
    '306': '306 - Long Island',
    '320': '320 - Riverside',
    '332': '332 - Western Minnesota',
    '344': '344 - Intl Food Group (IFG)-Jacksonville',
    '429': '429 - Doerle',
    '450': '450 - Alaska (NO DPM)'
}

# list of all north market sites (3 digit numbers)
north_market = ['007', '008', '009', '011', '012', '015', '018', '019', '024', '025', '027', '035', '038', '039', '047',
                '051', '054',
                '056', '058', '064', '068', '075', '076', '194', '195', '306', '332']

# list of all south market sites (3 digit numbers)
south_market = ['001', '002', '003', '006', '010', '013', '014', '016', '022', '023', '026', '029', '032', '037', '046',
                '048', '060',
                '067', '073', '078', '137', '163', '164', '288', '293', '429']

# list of all west market sites (3 digit numbers)
west_market = ['004', '005', '017', '031', '036', '040', '043', '045', '049', '050', '052', '055', '057', '059', '061',
               '066', '101',
               '102', '320', '450']

# list of all sites using the keys of all sites dictionary
all_sites = site_names.keys()


# function to establish connection to sus and return sus object
def sus(site):
    cnn_sus = pyodbc.connect(
        DRIVER='{IBM i Access ODBC Driver}',
        SYSTEM=f'AS{site}A',
        SIGNON=4,
        TRANSLATE=1,
        TRANSLATE_BINARY=True)

    return cnn_sus

