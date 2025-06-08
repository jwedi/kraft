#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CommandType {
    PUT = 80_u8, 
    DELETE = 68_u8, 
    #[default]
    NullVal = 0_u8, 
}
impl From<u8> for CommandType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            80_u8 => Self::PUT, 
            68_u8 => Self::DELETE, 
            _ => Self::NullVal,
        }
    }
}
impl From<CommandType> for u8 {
    #[inline]
    fn from(v: CommandType) -> Self {
        match v {
            CommandType::PUT => 80_u8, 
            CommandType::DELETE => 68_u8, 
            CommandType::NullVal => 0_u8,
        }
    }
}
impl core::str::FromStr for CommandType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "PUT" => Ok(Self::PUT), 
            "DELETE" => Ok(Self::DELETE), 
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for CommandType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PUT => write!(f, "PUT"), 
            Self::DELETE => write!(f, "DELETE"), 
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
