import { useState } from 'react'
import { Box, Button, Select, InputLabel, FormControl, Typography, TextField, MenuItem, RadioGroup, FormControlLabel, Radio } from '@mui/material'

function App() {
  const [count, setCount] = useState(0)

  const handleSubmit = async (event) => {
    event.preventDefault();

    const formData = new FormData(event.currentTarget);

    const data = {
      name: formData.get("Name"),
      email: formData.get("E-mejl"),
      dropdownType: formData.get("DropdownSelection"),
      bookingType: formData.get("BookingType")
    };

    console.log(data);
    console.log(JSON.stringify(data));

    try {
      const response = await fetch("/api/users", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        throw new Error("Request failed");
      }

      const result = await response.json();

      console.log("Success:", result);
    } catch (error) {
      console.error("Error:", error);
    }
  }

  return (
    <>
      <Box sx={{ display: 'flex', justifyContent: 'center' }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, maxWidth: '100%' }}>
          <Typography variant='h3'>Tours</Typography>

          <form onSubmit={handleSubmit}>
            <TextField id="outlined-basic" name="Name" label="Name" variant="outlined" />
            <TextField id="outlined-basic" name="E-mejl" label="E-mejl" variant="outlined" />

            <FormControl fullWidth>
              <InputLabel id="demo-simple-select-label">Tours</InputLabel>
              <Select
                name="DropdownSelection"
                labelId="demo-simple-select-label"
                id="demo-simple-select"
                // value={10}
                label="Age"
              // onChange={alert("Yo")}
              >
                <MenuItem value={10}>Ten</MenuItem>
                <MenuItem value={20}>Twenty</MenuItem>
                <MenuItem value={30}>Thirty</MenuItem>
              </Select>
            </FormControl>

            <FormControl>
              <RadioGroup row name="row-radio-buttons-group" name="BookingType">
                <FormControlLabel value="Book" control={<Radio />} label="Book" />
                <FormControlLabel value="Cancel" control={<Radio />} label="Cancel" />
              </RadioGroup>
            </FormControl>

            <Button variant='contained' type='Submit'>Send</Button>
          </form>
        </Box>
      </Box>

    </>
  )
}

export default App
